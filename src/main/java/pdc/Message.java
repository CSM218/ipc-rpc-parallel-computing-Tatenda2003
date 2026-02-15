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
    private static final int MAX_FIELD_BYTES = 100_000_000;

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
        if (length < 0 || length > MAX_FIELD_BYTES) {
            throw new IllegalArgumentException("Invalid frame length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String toJson() {
        validate();
        return "{"
                + "\"magic\":\"" + escapeJson(magic) + "\"," 
                + "\"version\":" + version + ","
                + "\"messageType\":\"" + escapeJson(messageType) + "\"," 
                + "\"studentId\":\"" + escapeJson(studentId) + "\"," 
                + "\"timestamp\":" + timestamp + ","
                + "\"payload\":\"" + escapeJson(payload) + "\""
                + "}";
    }

    public static Message parse(String json) {
        Message message = new Message();
        message.magic = extractString(json, "magic", MAGIC);
        message.version = extractInt(json, "version", CURRENT_VERSION);
        message.messageType = extractString(json, "messageType", "CONNECT");
        message.studentId = extractString(json, "studentId", "unknown");
        message.timestamp = extractLong(json, "timestamp", System.currentTimeMillis());
        message.payload = extractString(json, "payload", "");
        message.validate();
        return message;
    }

    private static String escapeJson(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String unescapeJson(String value) {
        return value
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
    }

    private static String extractString(String json, String key, String fallback) {
        String token = "\"" + key + "\":";
        int start = json.indexOf(token);
        if (start < 0) {
            return fallback;
        }
        int firstQuote = json.indexOf('"', start + token.length());
        if (firstQuote < 0) {
            return fallback;
        }
        int i = firstQuote + 1;
        boolean escaped = false;
        StringBuilder buffer = new StringBuilder();
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c == '"' && !escaped) {
                return unescapeJson(buffer.toString());
            }
            buffer.append(c);
            escaped = c == '\\' && !escaped;
            if (c != '\\') {
                escaped = false;
            }
            i++;
        }
        return fallback;
    }

    private static int extractInt(String json, String key, int fallback) {
        try {
            return (int) extractLong(json, key, fallback);
        } catch (Exception exception) {
            return fallback;
        }
    }

    private static long extractLong(String json, String key, long fallback) {
        String token = "\"" + key + "\":";
        int start = json.indexOf(token);
        if (start < 0) {
            return fallback;
        }
        int index = start + token.length();
        while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
            index++;
        }
        int end = index;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        if (end <= index) {
            return fallback;
        }
        return Long.parseLong(json.substring(index, end));
    }
}
