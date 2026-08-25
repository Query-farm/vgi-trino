// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.types;

import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
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
}
