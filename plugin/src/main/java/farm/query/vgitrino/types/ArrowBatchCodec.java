// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.function.Function;

/**
 * Encode/decode a full Arrow IPC record-batch stream (schema + one batch + EOS) — what VGI's
 * aggregate RPCs (see {@code VgiAggregateFunctions}) carry as a plain {@code byte[]} for {@code
 * aggregate_update}'s {@code input_batch}, {@code aggregate_finalize}'s {@code group_ids_batch}/
 * {@code result_batch}, and {@code aggregate_destructor}'s {@code group_ids_batch} — unlike scalar
 * functions, which never see a raw batch (they go through the exchange-mode streaming session
 * instead; see {@code VgiScalarFunctions#invoke}).
 *
 * <p>A small, self-contained helper built directly on Arrow's own {@link ArrowStreamWriter}/{@link
 * ArrowStreamReader} — the same "mirror the internal SDK helper rather than depend on it" reasoning
 * as {@link ArrowSchemaCodec} (whose {@code farm.query.vgi.internal.SchemaUtil} counterpart is an
 * internal, unstable-by-name package); here the internal counterpart is {@code
 * farm.query.vgi.internal.BatchUtil}.
 */
public final class ArrowBatchCodec {

    private ArrowBatchCodec() {}

    /**
     * Encode {@code root} as a one-batch IPC stream (schema message, one record-batch message, EOS).
     *
     * @param root the batch to encode; its current row count is what gets written
     * @return the encoded bytes
     */
    public static byte[] serialize(VectorSchemaRoot root) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to encode Arrow batch", e);
        }
    }

    /**
     * Decode a one-batch IPC stream and hand the resulting {@link VectorSchemaRoot} to {@code fn}
     * while the underlying reader is still open — {@link ArrowStreamReader#getVectorSchemaRoot()}
     * returns a root whose buffers the reader owns and releases on close, so reading every value
     * {@code fn} needs must happen before this method returns (mirrors {@code
     * farm.query.vgi.internal.BatchUtil#withReadBatch}'s own callback shape for exactly this
     * reason, not a stylistic choice).
     *
     * @param bytes the IPC stream bytes; {@code null}/empty or a stream with no batch at all calls
     *        {@code fn} with a {@code null} root
     * @param allocator the allocator backing the decoded vectors
     * @param fn what to do with the decoded root (or {@code null})
     * @return whatever {@code fn} returns
     */
    public static <T> T withReadBatch(byte[] bytes, BufferAllocator allocator, Function<VectorSchemaRoot, T> fn) {
        if (bytes == null || bytes.length == 0) return fn.apply(null);
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
            if (!reader.loadNextBatch()) return fn.apply(null);
            return fn.apply(reader.getVectorSchemaRoot());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to decode Arrow batch", e);
        }
    }
}
