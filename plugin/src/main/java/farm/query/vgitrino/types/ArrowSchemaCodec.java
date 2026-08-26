// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;

/**
 * Decode the Arrow IPC schema-stream bytes VGI carries in
 * {@code TableInfo.columns}, {@code BindResponse.output_schema}, and similar
 * fields.
 *
 * <p>A small, self-contained mirror of {@code farm.query.vgi.internal.SchemaUtil}
 * (an internal, unstable-by-name package in {@code farm.query:vgi}) rather than
 * a direct dependency on it — this connector only needs the read side, and
 * Arrow's own {@link MessageSerializer} is the actual implementation either
 * way.
 */
public final class ArrowSchemaCodec {

    private ArrowSchemaCodec() {}

    /**
     * Decode an Arrow IPC schema stream.
     *
     * @param bytes the IPC stream bytes; {@code null}/empty decodes to {@code null}
     * @return the decoded schema, or {@code null}
     */
    public static Schema deserializeSchema(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            ReadChannel rc = new ReadChannel(Channels.newChannel(in));
            return MessageSerializer.deserializeSchema(rc);
        } catch (Exception e) {
            throw new RuntimeException("failed to decode Arrow schema bytes", e);
        }
    }

    /**
     * Encode an Arrow schema as the IPC schema-message bytes VGI expects on the wire (e.g.
     * {@code BindRequest.input_schema}) — the write-side mirror of {@link #deserializeSchema}.
     */
    public static byte[] serializeSchema(Schema schema) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            WriteChannel wc = new WriteChannel(Channels.newChannel(out));
            MessageSerializer.serialize(wc, schema);
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("failed to encode Arrow schema", e);
        }
    }

    /**
     * Encode a whole one-batch {@link VectorSchemaRoot} (schema AND data) as
     * Arrow IPC bytes — unlike {@link #serializeSchema}, which carries no
     * rows. Used only to thread a table-in-out literal call's synthesized
     * 1-row input batch through a {@code ConnectorTableFunctionHandle} (Trino
     * serializes the handle across the coordinator/worker boundary, so the
     * literal argument values resolved at {@code analyze()} time must travel
     * as real bytes, not a live Java object) — see {@code
     * VgiTableInOutFunction#analyze}/{@code VgiTableInOutSplitProcessor}.
     *
     * @param root the batch to encode; not closed by this method
     * @return the schema-plus-one-record-batch IPC bytes
     */
    public static byte[] serializeBatch(VectorSchemaRoot root) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            WriteChannel wc = new WriteChannel(Channels.newChannel(out));
            MessageSerializer.serialize(wc, root.getSchema());
            try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
                MessageSerializer.serialize(wc, batch);
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("failed to encode Arrow batch", e);
        }
    }

    /**
     * Decode what {@link #serializeBatch} produced back into a fresh,
     * independently-owned {@link VectorSchemaRoot} (allocated from {@link
     * Allocators#root()}) — the caller owns and must close the returned root.
     *
     * @param bytes the schema-plus-one-record-batch IPC bytes
     * @return the decoded batch
     */
    public static VectorSchemaRoot deserializeBatch(byte[] bytes) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
            ReadChannel rc = new ReadChannel(Channels.newChannel(in));
            Schema schema = MessageSerializer.deserializeSchema(rc);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            try (ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(rc, Allocators.root())) {
                new VectorLoader(root).load(batch);
            }
            return root;
        } catch (Exception e) {
            throw new RuntimeException("failed to decode Arrow batch", e);
        }
    }
}
