// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Starts the real reference Python fixture worker against each transport
 * {@link farm.query.vgitrino.client.VgiWorkerClient} understands, and hands
 * back the {@code vgi.location} value plus a teardown — so the same ported
 * conformance content can run against subprocess, {@code unix://}, {@code
 * tcp://}, and {@code http(s)://} without each transport's test class
 * re-deriving its own spawn/discovery logic.
 *
 * <p>{@code unix}/{@code tcp} spawn the worker themselves and block-read its
 * stdout for the one discovery line {@code vgi/worker.py}'s {@code
 * Worker.main} emits on bind ({@code UNIX:<path>} / {@code TCP:<host>:<port>}),
 * symmetric to each other and to the launcher protocol's own discovery-line
 * contract — not a fixed {@code Thread.sleep()} poll, which is an acceptable
 * shortcut only for the hand-rolled Java TCP fixtures elsewhere in this test
 * tree, not for a real worker process whose startup time isn't a constant.
 */
public final class VgiWorkerHarness {

    private VgiWorkerHarness() {}

    /** A running (or, for {@code subprocess}, not-yet-started) worker: the {@code vgi.location} value to
     *  hand this connector, and a teardown to run afterward. */
    public record Handle(String location, AutoCloseable teardown) {}

    /** Bare command — this connector's own pool spawns one subprocess per pooled connection. */
    public static Handle subprocess(java.io.File vgiPythonDir) {
        return new Handle("uv run --project " + vgiPythonDir.getAbsolutePath() + " vgi-fixture-worker",
                () -> {});
    }

    /** One real worker process listening on a fresh temp-directory Unix domain socket. */
    public static Handle unix(java.io.File vgiPythonDir) throws IOException {
        Path socketDir = Files.createTempDirectory("vgi-trino-unix-");
        Path socketPath = socketDir.resolve("w.sock");
        Process worker = new ProcessBuilder("uv", "run", "--project", vgiPythonDir.getAbsolutePath(),
                "vgi-fixture-worker", "--unix", socketPath.toString())
                .directory(vgiPythonDir)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        awaitDiscoveryLine(worker, "UNIX:");
        return new Handle("unix://" + socketPath, () -> {
            worker.destroy();
            Files.deleteIfExists(socketPath);
            Files.deleteIfExists(socketDir);
        });
    }

    /** One real worker process listening on an auto-selected TCP port. */
    public static Handle tcp(java.io.File vgiPythonDir) throws IOException {
        Process worker = new ProcessBuilder("uv", "run", "--project", vgiPythonDir.getAbsolutePath(),
                "vgi-fixture-worker", "--tcp", "0")
                .directory(vgiPythonDir)
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .start();
        String discovery = awaitDiscoveryLine(worker, "TCP:");
        String hostPort = discovery.substring("TCP:".length());
        return new Handle("tcp://" + hostPort, worker::destroy);
    }

    /** One real worker process serving HTTP, port discovered via {@code --port-file}'s atomic write —
     *  the same mechanism the C++ VGI extension's own test harness uses. */
    public static Handle http(java.io.File vgiPythonDir) throws IOException, InterruptedException {
        Path portFile = Files.createTempFile("vgi-trino-http-", ".port");
        Files.deleteIfExists(portFile);
        Process worker = new ProcessBuilder("uv", "run", "--project", vgiPythonDir.getAbsolutePath(),
                "vgi-fixture-http", "--port", "0", "--port-file", portFile.toString())
                .directory(vgiPythonDir)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
        int port = -1;
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(portFile)) {
                String content = Files.readString(portFile).strip();
                if (!content.isEmpty()) {
                    port = Integer.parseInt(content);
                    break;
                }
            }
            if (!worker.isAlive()) {
                throw new IllegalStateException(
                        "vgi-fixture-http exited before writing its port file (exit code "
                                + worker.exitValue() + ")");
            }
            Thread.sleep(200);
        }
        Files.deleteIfExists(portFile);
        if (port <= 0) {
            worker.destroy();
            throw new IllegalStateException("timed out waiting for vgi-fixture-http to report its bound port");
        }
        int boundPort = port;
        return new Handle("http://127.0.0.1:" + boundPort, worker::destroy);
    }

    /**
     * Block-read {@code worker}'s stdout for a line starting with {@code prefix}, skipping anything
     * else (banner noise, warnings) up to a byte cap — mirrors the launcher protocol's own 1 MiB
     * pre-discovery noise allowance — then drains the rest of stdout on a daemon thread so the child
     * never blocks on a full pipe after the line we care about has been read.
     */
    private static String awaitDiscoveryLine(Process worker, String prefix) throws IOException {
        BlockingQueue<Object> lines = new ArrayBlockingQueue<>(64); // String lines, or a Throwable on failure
        Thread reader = new Thread(() -> {
            long bytesRead = 0;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    bytesRead += line.length() + 1;
                    if (bytesRead > 1_048_576) {
                        lines.offer(new IOException("exceeded 1 MiB of stdout without a " + prefix + " line"));
                        return;
                    }
                    lines.offer(line);
                    if (line.startsWith(prefix)) {
                        // Keep draining afterward so the child never blocks on a full stdout pipe.
                        while (in.readLine() != null) { /* discard */ }
                        return;
                    }
                }
                lines.offer(new IOException("worker's stdout closed before a " + prefix + " line appeared"));
            } catch (IOException e) {
                lines.offer(e);
            }
        }, "vgi-worker-harness-discovery");
        reader.setDaemon(true);
        reader.start();

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (!worker.isAlive()) {
                throw new IOException("worker exited before a " + prefix + " line appeared (exit code "
                        + worker.exitValue() + ")");
            }
            Object next;
            try {
                next = lines.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for a " + prefix + " line", e);
            }
            if (next instanceof IOException e) throw e;
            if (next instanceof String s && s.startsWith(prefix)) return s;
        }
        worker.destroy();
        throw new IOException("timed out waiting for a " + prefix + " line on the worker's stdout");
    }
}
