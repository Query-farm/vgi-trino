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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
 * <p>A connection that throws is evicted rather than returned to the pool —
 * VGI's lockstep framing means a call that failed mid-stream may have left the
 * wire in an indeterminate state, and reusing it would corrupt the next call
 * rather than merely fail it. v1 does not respawn evicted connections; a
 * worker that crashes repeatedly will shrink the pool towards zero rather than
 * self-heal. Tracked as a follow-up, not a v1 blocker.
 */
public final class VgiWorkerClient implements AutoCloseable {

    /** One pooled, attached connection. */
    public record Attached(RpcConnection connection, VgiService service, CatalogAttachResult attach) {
        /** @return the {@code attach_opaque_data} handle every subsequent call on this connection echoes */
        public byte[] handle() { return attach.attach_opaque_data(); }
    }

    private final VgiConfig config;
    private final BlockingQueue<Attached> pool = new LinkedBlockingQueue<>();
    private final List<Attached> all;

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
        this.all = List.copyOf(opened);
    }

    /** @return this client's configuration */
    public VgiConfig config() { return config; }

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
     * @return a connection from the pool, blocking until one is available
     */
    public Attached borrow() {
        try {
            return pool.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted waiting for a VGI worker connection", e);
        }
    }

    /**
     * Return a borrowed connection: to the pool if {@code healthy}, or evict
     * and close it otherwise (VGI's lockstep framing means a connection a call
     * failed against may be left in an indeterminate wire state — reusing it
     * would corrupt the next call rather than merely fail it).
     *
     * @param a the connection {@link #borrow} returned
     * @param healthy whether every call made against it completed cleanly
     */
    public void release(Attached a, boolean healthy) {
        if (healthy) {
            pool.offer(a);
        } else {
            closeQuietly(a);
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
    }
}
