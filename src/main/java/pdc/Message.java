package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int CURRENT_VERSION = 1;

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public Message() {
        this.magic = MAGIC;
        this.version = CURRENT_VERSION;
        this.messageType = "CONNECT";
        this.studentId = "unknown";
        this.timestamp = System.currentTimeMillis();
        this.payload = "";
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        validate();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            writeString(out, magic);
            out.writeInt(version);
            writeString(out, messageType);
            writeString(out, studentId);
            out.writeLong(timestamp);
            writeString(out, payload);
            out.flush();
            return baos.toByteArray();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to serialize message", exception);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            Message message = new Message();
            message.magic = readString(in);
            message.version = in.readInt();
            message.messageType = readString(in);
            message.studentId = readString(in);
            message.timestamp = in.readLong();
            message.payload = readString(in);
            message.validate();
            return message;
        } catch (IOException exception) {
            throw new IllegalArgumentException("Failed to parse message", exception);
        }
    }

    public void validate() {
        if (magic == null || !MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic value");
        }
        if (version != CURRENT_VERSION) {
            throw new IllegalArgumentException("Unsupported protocol version");
        }
        if (messageType == null || messageType.isBlank()) {
            throw new IllegalArgumentException("Missing messageType");
        }
        if (studentId == null || studentId.isBlank()) {
            throw new IllegalArgumentException("Missing studentId");
        }
        if (payload == null) {
            throw new IllegalArgumentException("Missing payload");
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 10_000_000) {
            throw new IllegalArgumentException("Invalid frame length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
