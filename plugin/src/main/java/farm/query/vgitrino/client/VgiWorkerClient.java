// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.launcher.LaunchConfig;
import farm.query.vgirpc.launcher.LauncherClient;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    /**
     * One pooled, attached connection.
     *
     * @param connection {@link RpcConnection} for subprocess/{@code unix://}/
     *        {@code tcp://}, {@link HttpRpcConnection} for {@code http(s)://}
     *        — {@code AutoCloseable} is the only thing the rest of this class
     *        needs from it (see {@link #closeQuietly}); every actual RPC call
     *        goes through {@link #service} instead, which is transport-agnostic
     *        already (both connection types offer the identical {@code
     *        proxy(Class)} surface)
     */
    public record Attached(AutoCloseable connection, VgiService service, CatalogAttachResult attach) {
        /** @return the {@code attach_opaque_data} handle every subsequent call on this connection echoes */
        public byte[] handle() { return attach.attach_opaque_data(); }
    }

    private final VgiConfig config;
    // Guards both deques below. A plain monitor, not a BlockingQueue: async
    // acquisition (borrowAsync) needs to hand an available connection
    // straight to a WAITING FUTURE without any thread blocking to receive
    // it, which a blocking queue's own API has no way to express.
    private final Object lock = new Object();
    private final Deque<Attached> available = new ArrayDeque<>();
    private final Deque<CompletableFuture<Attached>> waiters = new ArrayDeque<>();
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
                opened.add(openAndAttach());
            }
        } catch (RuntimeException e) {
            for (Attached a : opened) closeQuietly(a);
            throw e;
        }
        available.addAll(opened);
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
     * forever, but should only be used for calls that happen ONCE per query
     * (catalog/table metadata, a scan's own bind+plan) — code that can run
     * concurrently, once per SPLIT, must use {@link #borrowAsync} instead
     * (see {@code VgiPageSource}/{@code VgiTableFunctionSplitProcessor}), or
     * enough concurrently-scheduled splits will queue real Trino engine
     * threads behind this bounded wait rather than yielding them back to the
     * engine while genuinely idle.
     *
     * @return a connection from the pool
     * @throws RuntimeException if none becomes available within the
     *         configured timeout, or the wait is interrupted
     */
    public Attached borrow() {
        CompletableFuture<Attached> future = borrowAsync();
        try {
            return future.get(config.connectionAcquireTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Nobody else will ever collect from `future` — if it slipped
            // through despite withdrawal (raced a concurrent release()),
            // reclaim the connection into the pool rather than lose it.
            cancelPendingBorrow(future);
            future.thenAccept(this::offer);
            throw new RuntimeException(new TimeoutException(
                    "timed out after " + config.connectionAcquireTimeoutMillis()
                            + "ms waiting for a pooled VGI worker connection ("
                            + "vgi.connections=" + config.connections() + " may be too small for how many "
                            + "splits this query redeems concurrently, or the worker may be stuck — raise "
                            + "vgi.connection-acquire-timeout-millis or vgi.connections if this is expected)"));
        } catch (InterruptedException e) {
            cancelPendingBorrow(future);
            future.thenAccept(this::offer);
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted waiting for a VGI worker connection", e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException re ? re : new RuntimeException(e.getCause());
        }
    }

    /**
     * Reserve a connection without blocking the calling thread: completes
     * immediately if one is already available, otherwise returns an
     * incomplete future that a later {@link #release} call will complete —
     * the async counterpart to {@link #borrow}, for code that must not
     * occupy a Trino engine thread just to wait its turn (Trino may schedule
     * more concurrent splits than {@link VgiConfig#connections()} allows —
     * e.g. a {@code LIMIT} satisfiable from the very first split still
     * starts redeeming others in parallel before the engine notices it has
     * enough — and the resulting excess must queue as pure data, not as
     * blocked threads). Never times out on its own; a caller that needs a
     * bound should race it against a delay of its own choosing.
     *
     * @return a future for a connection from the pool
     */
    public CompletableFuture<Attached> borrowAsync() {
        synchronized (lock) {
            Attached a = available.poll();
            if (a != null) return CompletableFuture.completedFuture(a);
            CompletableFuture<Attached> future = new CompletableFuture<>();
            waiters.add(future);
            return future;
        }
    }

    /**
     * Withdraw an unwanted wait from {@link #waiters}: for a {@link #borrow}
     * caller giving up (timeout, interruption), or a {@code VgiPageSource}/
     * {@code VgiTableFunctionSplitProcessor} whose split was closed/cancelled
     * before {@link #borrowAsync} ever handed it a connection. Without this,
     * an abandoned waiter sits in {@link #waiters} forever — not leaking a
     * real connection (nothing is holding one), but capable of stealing a
     * LATER {@link #release} from whichever active caller actually needs it,
     * since {@link #offer} has no way to tell a live waiter from a dead one.
     *
     * <p>Only removes the entry — does NOT reclaim a connection that slipped
     * through despite the withdrawal (a race against a concurrent {@link
     * #release} completing this exact future in the gap before this call
     * runs). {@link #borrow}'s own callers own that decision for themselves
     * (nobody else will ever collect from their future, so they reclaim
     * unconditionally); a page source/split processor's {@code close()} owns
     * it too, but differently — it may have ALREADY started redeeming that
     * connection by the time it notices the close, and must not race its own
     * in-flight {@code init()} by also handing the same connection back here.
     * A shared, unconditional reclaim in this one method can't tell those
     * two situations apart, so it isn't this method's job.
     *
     * @param future a future this same client's {@link #borrowAsync} returned
     */
    public void cancelPendingBorrow(CompletableFuture<Attached> future) {
        synchronized (lock) {
            waiters.remove(future);
        }
    }

    /**
     * Return a borrowed connection: to the pool (or straight to a waiting
     * {@link #borrowAsync} caller) if {@code healthy}, or evict, close it,
     * and mint a fresh replacement otherwise (VGI's lockstep framing means a
     * connection a call failed against may be left in an indeterminate wire
     * state — reusing it would corrupt the next call rather than merely
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
     * @param a the connection {@link #borrow}/{@link #borrowAsync} returned
     * @param healthy whether every call made against it completed cleanly
     */
    public void release(Attached a, boolean healthy) {
        if (healthy) {
            offer(a);
            return;
        }
        closeQuietly(a);
        all.remove(a);
        try {
            Attached replacement = openAndAttach();
            all.add(replacement);
            offer(replacement);
        } catch (RuntimeException e) {
            // The worker looks genuinely down: nothing to put back. Losing
            // one pool slot here is an honest degradation (this catalog now
            // has one fewer connection to redeem splits with), not a hang —
            // unlike silently coming up short forever, later borrow() calls
            // still succeed against the remaining connections and only run
            // out if EVERY one of them has failed the same way.
        }
    }

    /**
     * Hand a connection straight to the longest-waiting {@link #borrowAsync}
     * caller, or park it in {@link #available} if there isn't one.
     * {@code CompletableFuture.complete} runs any callbacks already chained
     * onto that future (a page source's own {@code thenApplyAsync}, say)
     * synchronously on THIS thread unless they were chained with an
     * executor-qualified variant — which is exactly why every caller of
     * {@code borrowAsync} in this connector chains onward with {@code
     * thenApplyAsync(..., executor())}, never a bare {@code thenApply}: this
     * method runs from inside {@link #release}, called from arbitrary
     * connector threads (another split's teardown, this class's own
     * constructor-time replacement logic), and none of them should end up
     * unexpectedly running a DIFFERENT split's {@code init()} RPC.
     */
    private void offer(Attached a) {
        CompletableFuture<Attached> waiter;
        synchronized (lock) {
            waiter = waiters.poll();
            if (waiter == null) {
                available.add(a);
                return;
            }
        }
        if (!waiter.complete(a)) {
            // Lost a race with abandon() cancelling/discarding this exact
            // waiter between poll() and complete() — it has no taker now,
            // so put it back rather than let it vanish.
            offer(a);
        }
    }

    private Attached openAndAttach() {
        String location = config.location();
        if (location.startsWith("http://") || location.startsWith("https://")) {
            return openAndAttachHttp(location);
        }
        RpcTransport transport = openTransport(location);
        RpcConnection connection = new RpcConnection(transport);
        VgiService service = connection.proxy(VgiService.class);
        CatalogAttachResult attach = service.catalog_attach(
                CatalogAttachRequest.of(config.catalogName(), null, null, null), null);
        return new Attached(connection, service, attach);
    }

    /**
     * {@code http(s)://} has no {@link RpcTransport} — it's a chain of
     * independent request/response pairs, not a duplex byte stream — so it
     * gets its own connection type ({@link HttpRpcConnection}) entirely,
     * built directly rather than through {@link #openTransport}. Each pooled
     * {@link Attached} still does its own {@code catalog_attach}, exactly
     * like the byte-stream transports, even though an {@code
     * HttpRpcConnection} is itself safe to share across concurrent calls —
     * pooling {@link VgiConfig#connections()} separate instances anyway
     * keeps this transport on the SAME acquire/release/self-heal machinery
     * every other transport already goes through, rather than forking a
     * second connection-lifecycle model for HTTP alone. (A single shared
     * instance really could serve unlimited concurrent splits over HTTP —
     * see the README's Connection acquisition section for why that's a
     * documented follow-up, not done here.)
     */
    private Attached openAndAttachHttp(String location) {
        HttpRpcConnection.Builder builder = HttpRpcConnection.builder(location);
        if (config.httpBearerToken() != null) {
            builder.bearerToken(config.httpBearerToken());
        }
        HttpRpcConnection connection = builder.build();
        boolean ok = false;
        try {
            VgiService service = connection.proxy(VgiService.class);
            CatalogAttachResult attach = service.catalog_attach(
                    CatalogAttachRequest.of(config.catalogName(), null, null, null), null);
            Attached a = new Attached(connection, service, attach);
            ok = true;
            return a;
        } finally {
            if (!ok) closeQuietly(connection);
        }
    }

    private static RpcTransport openTransport(String location) {
        if (location.startsWith("unix://")) {
            String path = location.substring("unix://".length());
            return connectUnixSocket(path);
        }
        if (location.startsWith("launch:")) {
            // A launch: location resolves to a warm, shared worker's unix socket (spawning it
            // if none is running yet) and connects to it exactly like a plain unix:// location —
            // launch: IS unix://, just with the spawn-if-needed step folded in first. See the
            // README's Scope section for what this doesn't (yet) do: per-location idle-timeout/
            // state-dir overrides (the ATTACH options the C++ extension exposes for this), and
            // Windows (launch: itself has no Windows implementation in vgi-rpc-java either).
            List<String> argv = LaunchLocationParser.parseArgv(location.substring("launch:".length()));
            String socketPath;
            try {
                socketPath = LauncherClient.launch(LaunchConfig.of(argv));
            } catch (IOException e) {
                throw new UncheckedIOException("failed to launch VGI worker for " + location, e);
            }
            return connectUnixSocket(socketPath);
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
        // Bare command: run through a shell so the operator's own quoting,
        // env expansion, and PATH lookup behave the way it would on a
        // terminal — matches the DuckDB extension's own LOCATION contract.
        return new SubprocessTransport(List.of("/bin/sh", "-c", location));
    }

    private static RpcTransport connectUnixSocket(String path) {
        try {
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            channel.connect(UnixDomainSocketAddress.of(path));
            return new UnixSocketTransport(channel);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to connect to VGI unix socket " + path, e);
        }
    }

    private static void closeQuietly(Attached a) {
        closeQuietly(a.connection());
    }

    private static void closeQuietly(AutoCloseable a) {
        try {
            a.close();
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
