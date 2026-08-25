// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.SubprocessTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;
import farm.query.vgitrino.VgiConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A pool of independent connections to one VGI worker, each attached to the
 * same VGI-side catalog.
 *
 * <p>VGI's RPC is lockstep per connection — one call in flight at a time, the
 * same constraint that makes DuckDB pool subprocess workers rather than share
 * one — so redeeming {@code N} Trino splits concurrently needs {@code N}
 * independent connections, not one shared client. This pool pre-spawns
 * {@link VgiConfig#connections()} of them at construction (each doing its own
 * {@code catalog_attach}) and hands them out via {@link #withConnection}.
 *
 * <p>A connection that throws is never returned to the pool as-is — VGI's
 * lockstep framing means a call that failed mid-stream may have left the wire
 * in an indeterminate state, and reusing it would corrupt the next call
 * rather than merely fail it. {@link #release} closes it and immediately
 * opens+attaches a fresh replacement in its place, so the POOL SIZE is
 * self-healing rather than monotonically shrinking. Not doing this was v1's
 * original design (evict and never replace) — found, by porting the VGI C++
 * suite's own {@code splits/poisoned_conn.test}/{@code errors.test} against
 * this connector, to be a real, silent deadlock rather than a mere throughput
 * degradation: with {@code connections=N}, N cumulative connection failures
 * over the catalog attachment's LIFETIME (not per-query) drained the pool to
 * zero, and every subsequent {@link #borrow} — on a completely unrelated,
 * otherwise-healthy query — blocked forever on an empty queue that nothing
 * would ever refill. A worker that is genuinely down still degrades honestly:
 * if the replacement attach itself fails, that one slot is lost (logged
 * failures compound instead of manufacturing fake capacity), rather than
 * retried in a loop that could itself hang {@link #release}.
 */
public final class VgiWorkerClient implements AutoCloseable {

    /** One pooled, attached connection. */
    public record Attached(RpcConnection connection, VgiService service, CatalogAttachResult attach) {
        /** @return the {@code attach_opaque_data} handle every subsequent call on this connection echoes */
        public byte[] handle() { return attach.attach_opaque_data(); }
    }

    private final VgiConfig config;
    private final BlockingQueue<Attached> pool = new LinkedBlockingQueue<>();
    // Every live connection this client has ever opened, for close() to shut
    // down — including self-healing replacements minted by release(), which
    // is why this can't be the fixed List.copyOf snapshot construction alone
    // produces. Concurrent because release() (many splits, many threads) and
    // close() (one, at shutdown) touch it independently of the pool queue.
    private final Queue<Attached> all = new ConcurrentLinkedQueue<>();
    // A dedicated pool for VgiSplitSource's async getNextBatch — deliberately
    // NOT CompletableFuture.supplyAsync's default (the JVM-wide common
    // ForkJoinPool). That pool is shared across the ENTIRE JVM, including
    // whatever else Trino's own internals use it for; a plugin submitting
    // its own work onto it — however briefly — has no way to guarantee its
    // own classloader never ends up as a common-pool worker thread's context
    // classloader in some way that outlives this one call (a plain Thread
    // copies its creator's context classloader once, at creation, and
    // nothing resets it on later reuse for unrelated work). A connector-
    // private pool makes that categorically impossible: nothing but this
    // connector's own work ever runs on these threads. (The specific crash
    // that motivated auditing this — `ClassNotFoundException: com.fasterxml.
    // jackson.module.blackbird.deser.CreatorOptimizer`, reproduced against a
    // real `trinodb/trino:483` image, see docker/docker-compose.yml — turned
    // out to have a different, unrelated root cause: Trino's own task-update
    // wire protocol needing blackbird resolvable from THIS plugin's
    // classloader to deserialize this connector's own SPI types, fixed via
    // the plugin/build.gradle.kts dependency + version-alignment comments.
    // This executor is still worth keeping on its own merits — depending on
    // the JVM-wide common pool from connector code is a latent risk
    // regardless of whether it was the cause here.)
    private final ExecutorService executor = Executors.newCachedThreadPool(
            runnable -> {
                Thread t = new Thread(runnable, "vgi-trino-split-source");
                t.setDaemon(true);
                return t;
            });

    /**
     * Spawn/connect {@link VgiConfig#connections()} independent connections and
     * attach each one.
     *
     * @param config the catalog's parsed configuration
     */
    public VgiWorkerClient(VgiConfig config) {
        this.config = config;
        int n = Math.max(1, config.connections());
        List<Attached> opened = new ArrayList<>(n);
        try {
            for (int i = 0; i < n; i++) {
                Attached a = openAndAttach();
                opened.add(a);
                pool.add(a);
            }
        } catch (RuntimeException e) {
            for (Attached a : opened) closeQuietly(a);
            throw e;
        }
        all.addAll(opened);
    }

    /** @return this client's configuration */
    public VgiConfig config() { return config; }

    /**
     * @return the dedicated executor {@link farm.query.vgitrino.split.VgiSplitSource}
     *         must use for its async {@code getNextBatch} — never the JVM-wide
     *         common {@code ForkJoinPool} (see this field's own javadoc for why)
     */
    public ExecutorService executor() { return executor; }

    /**
     * Borrow a connection, run {@code fn} against it, and return it to the
     * pool — or evict it if {@code fn} threw.
     *
     * <p>For a call that needs to hold a connection across more than one
     * synchronous operation — a page source draining a producer stream tick
     * by tick — use {@link #borrow}/{@link #release} directly instead.
     *
     * @param fn the RPC calls to make against one connection
     * @param <T> the result type
     * @return whatever {@code fn} returned
     */
    public <T> T withConnection(Function<Attached, T> fn) {
        Attached a = borrow();
        boolean healthy = false;
        try {
            T result = fn.apply(a);
            healthy = true;
            return result;
        } finally {
            release(a, healthy);
        }
    }

    /**
     * Borrow a connection for the caller to hold across multiple operations.
     * Must be paired with exactly one {@link #release} call.
     *
     * <p>Blocks up to {@link VgiConfig#connectionAcquireTimeoutMillis()}, not
     * forever. Trino's split-processor/page-source construction is expected
     * to be cheap and non-blocking (the SPI's own async escape hatches —
     * {@code ConnectorPageSource.isBlocked()},
     * {@code TableFunctionProcessorState.Blocked} — exist for exactly the
     * case this connector doesn't yet use them for), so it may legitimately
     * schedule more concurrent splits than {@link VgiConfig#connections()}
     * allows — e.g. a {@code LIMIT} satisfiable from the very first split
     * still starts redeeming others in parallel before the engine notices it
     * has enough. Those extras block here, behind whichever connections are
     * already busy, exactly as intended — a bounded wait is what keeps a
     * connector or worker that's genuinely stuck (nothing left that will ever
     * free one) from hanging the calling Trino engine thread, and every other
     * query sharing whatever thread pool that thread came from, forever.
     *
     * @return a connection from the pool
     * @throws RuntimeException if none becomes available within the
     *         configured timeout, or the wait is interrupted
     */
    public Attached borrow() {
        try {
            Attached a = pool.poll(config.connectionAcquireTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (a == null) {
                throw new RuntimeException(new TimeoutException(
                        "timed out after " + config.connectionAcquireTimeoutMillis()
                                + "ms waiting for a pooled VGI worker connection ("
                                + "vgi.connections=" + config.connections() + " may be too small for how many "
                                + "splits this query redeems concurrently, or the worker may be stuck — raise "
                                + "vgi.connection-acquire-timeout-millis or vgi.connections if this is expected)"));
            }
            return a;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted waiting for a VGI worker connection", e);
        }
    }

    /**
     * Return a borrowed connection: to the pool if {@code healthy}, or evict,
     * close it, and mint a fresh replacement otherwise (VGI's lockstep framing
     * means a connection a call failed against may be left in an indeterminate
     * wire state — reusing it would corrupt the next call rather than merely
     * fail it).
     *
     * <p>The replacement is what keeps the pool's SIZE stable across a
     * transient failure — a fixture or worker bug that fails one split's
     * {@code init()} must not cost every later query on this catalog a
     * permanent slot; enough of those over the catalog's lifetime would
     * otherwise drain the pool to zero and hang every subsequent
     * {@link #borrow}. If re-attaching itself fails, the worker is presumably
     * genuinely unreachable — that slot is honestly lost rather than retried
     * in a loop that could hang this call.
     *
     * @param a the connection {@link #borrow} returned
     * @param healthy whether every call made against it completed cleanly
     */
    public void release(Attached a, boolean healthy) {
        if (healthy) {
            pool.offer(a);
            return;
        }
        closeQuietly(a);
        all.remove(a);
        try {
            Attached replacement = openAndAttach();
            all.add(replacement);
            pool.offer(replacement);
        } catch (RuntimeException e) {
            // The worker looks genuinely down: nothing to put back. Losing
            // one pool slot here is an honest degradation (this catalog now
            // has one fewer connection to redeem splits with), not a hang —
            // unlike silently coming up short forever, later borrow() calls
            // still succeed against the remaining connections and only run
            // out if EVERY one of them has failed the same way.
        }
    }

    private Attached openAndAttach() {
        RpcTransport transport = openTransport(config.location());
        RpcConnection connection = new RpcConnection(transport);
        VgiService service = connection.proxy(VgiService.class);
        CatalogAttachResult attach = service.catalog_attach(
                CatalogAttachRequest.of(config.catalogName(), null, null, null), null);
        return new Attached(connection, service, attach);
    }

    private static RpcTransport openTransport(String location) {
        if (location.startsWith("unix://")) {
            String path = location.substring("unix://".length());
            try {
                SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
                channel.connect(UnixDomainSocketAddress.of(path));
                return new UnixSocketTransport(channel);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to connect to VGI unix socket " + path, e);
            }
        }
        if (location.startsWith("tcp://")) {
            URI uri = URI.create(location);
            try {
                Socket socket = new Socket(uri.getHost(), uri.getPort());
                return new TcpSocketTransport(socket);
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "failed to connect to VGI worker at " + uri.getHost() + ":" + uri.getPort(), e);
            }
        }
        if (location.startsWith("http://") || location.startsWith("https://")) {
            throw new UnsupportedOperationException(
                    "vgi.location HTTP transport is not yet implemented — use a subprocess "
                            + "command, unix://path, or tcp://host:port. See the connector README.");
        }
        // Bare command: run through a shell so the operator's own quoting,
        // env expansion, and PATH lookup behave the way it would on a
        // terminal — matches the DuckDB extension's own LOCATION contract.
        return new SubprocessTransport(List.of("/bin/sh", "-c", location));
    }

    private static void closeQuietly(Attached a) {
        try {
            a.connection().close();
        } catch (Exception ignore) {
            // best-effort — the pool is shrinking either way
        }
    }

    @Override
    public void close() {
        for (Attached a : all) closeQuietly(a);
        executor.shutdownNow();
    }
}
