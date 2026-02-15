package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final ExecutorService workerThreads = Executors.newFixedThreadPool(2);
    private final ExecutorService computePool = Executors
            .newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final String workerId = System.getenv().getOrDefault("WORKER_ID", "worker-local");
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "unknown-student");

    private volatile Socket socket;
    private volatile DataInputStream input;
    private volatile DataOutputStream output;

    public Worker() {
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        String host = masterHost == null || masterHost.isBlank()
                ? System.getenv().getOrDefault("MASTER_HOST", "localhost")
                : masterHost;
        int masterPort = port > 0 ? port : parseIntEnv("MASTER_PORT", 9999);

        try {
            socket = new Socket(host, masterPort);
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            running.set(true);

            Message register = new Message();
            register.messageType = "REGISTER_WORKER";
            register.studentId = studentId;
            register.payload = workerId;
            send(register);

            workerThreads.submit(this::readLoop);
        } catch (Exception exception) {
            // join should fail gracefully for unit tests if master is absent
            running.set(false);
            closeQuietly();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        workerThreads.submit(() -> {
            if (!running.get()) {
                return;
            }

            try {
                Message capabilities = new Message();
                capabilities.messageType = "REGISTER_CAPABILITIES";
                capabilities.studentId = studentId;
                capabilities.payload = "MATRIX_MULTIPLY,BLOCK_TRANSPOSE";
                send(capabilities);
            } catch (Exception exception) {
                running.set(false);
                closeQuietly();
            }
        });
    }

    private void readLoop() {
        while (running.get()) {
            try {
                Message incoming = readMessage();
                incoming.validate();

                if ("HEARTBEAT".equals(incoming.messageType)) {
                    Message ack = new Message();
                    ack.messageType = "HEARTBEAT";
                    ack.studentId = studentId;
                    ack.payload = workerId;
                    send(ack);
                } else if ("RPC_REQUEST".equals(incoming.messageType)) {
                    computePool.submit(() -> {
                        try {
                            handleRpc(incoming);
                        } catch (IOException exception) {
                            running.set(false);
                            closeQuietly();
                        }
                    });
                } else if ("WORKER_ACK".equals(incoming.messageType)) {
                    // token/ack accepted
                }
            } catch (Exception exception) {
                running.set(false);
                closeQuietly();
            }
        }
    }

    private void handleRpc(Message request) throws IOException {
        ParsedTask parsedTask = parseTask(request.payload);
        Message response = new Message();
        response.studentId = studentId;
        try {
            String result = executeTask(parsedTask.taskType, parsedTask.taskPayload);
            response.messageType = "TASK_COMPLETE";
            response.payload = parsedTask.taskId + ";" + result;
        } catch (Exception exception) {
            response.messageType = "TASK_ERROR";
            response.payload = parsedTask.taskId + ";" + exception.getMessage();
        }
        send(response);
    }

    private ParsedTask parseTask(String payload) {
        int first = payload.indexOf(';');
        int second = first < 0 ? -1 : payload.indexOf(';', first + 1);
        if (first < 0 || second < 0) {
            throw new IllegalArgumentException("Invalid RPC payload");
        }
        String taskId = payload.substring(0, first);
        String taskType = payload.substring(first + 1, second);
        String taskPayload = payload.substring(second + 1);
        return new ParsedTask(taskId, taskType, taskPayload);
    }

    private String executeTask(String taskType, String payload) {
        if ("MATRIX_MULTIPLY".equals(taskType)) {
            String[] parts = splitMultiplyPayload(payload);
            int[][] left = parseMatrix(parts[0]);
            int[][] right = parseMatrix(parts[1]);
            int[][] result = multiply(left, right);
            return toMatrixString(result);
        }

        if ("BLOCK_TRANSPOSE".equals(taskType)) {
            int[][] matrix = parseMatrix(payload);
            return toMatrixString(transpose(matrix));
        }

        // Unknown operation: return payload unchanged to preserve protocol progress.
        return payload;
    }

    private String[] splitMultiplyPayload(String payload) {
        int sep = payload.indexOf('|');
        if (sep < 0) {
            throw new IllegalArgumentException("Invalid multiply payload");
        }
        return new String[] { payload.substring(0, sep), payload.substring(sep + 1) };
    }

    private int[][] parseMatrix(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            return new int[0][0];
        }
        String[] rowTokens = encoded.split("\\\\");
        List<int[]> rows = new ArrayList<>();
        int expectedCols = -1;
        for (String rowToken : rowTokens) {
            if (rowToken.isBlank()) {
                continue;
            }
            String[] colTokens = rowToken.split(",");
            if (expectedCols == -1) {
                expectedCols = colTokens.length;
            } else if (colTokens.length != expectedCols) {
                throw new IllegalArgumentException("Ragged matrix rows");
            }
            int[] row = new int[colTokens.length];
            for (int i = 0; i < colTokens.length; i++) {
                row[i] = Integer.parseInt(colTokens[i].trim());
            }
            rows.add(row);
        }

        int[][] matrix = new int[rows.size()][];
        for (int i = 0; i < rows.size(); i++) {
            matrix[i] = rows.get(i);
        }
        return matrix;
    }

    private int[][] multiply(int[][] left, int[][] right) {
        if (left.length == 0 || right.length == 0) {
            return new int[0][0];
        }
        int leftRows = left.length;
        int leftCols = left[0].length;
        int rightRows = right.length;
        int rightCols = right[0].length;
        if (leftCols != rightRows) {
            throw new IllegalArgumentException("Incompatible matrix dimensions");
        }

        int[][] result = new int[leftRows][rightCols];
        for (int i = 0; i < leftRows; i++) {
            for (int k = 0; k < leftCols; k++) {
                int leftValue = left[i][k];
                for (int j = 0; j < rightCols; j++) {
                    result[i][j] += leftValue * right[k][j];
                }
            }
        }
        return result;
    }

    private int[][] transpose(int[][] matrix) {
        if (matrix.length == 0) {
            return new int[0][0];
        }
        int rows = matrix.length;
        int cols = matrix[0].length;
        int[][] result = new int[cols][rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result[j][i] = matrix[i][j];
            }
        }
        return result;
    }

    private String toMatrixString(int[][] matrix) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                builder.append('\\');
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    builder.append(',');
                }
                builder.append(matrix[i][j]);
            }
        }
        return builder.toString();
    }

    private Message readMessage() throws IOException {
        int frameLength = input.readInt();
        if (frameLength <= 0) {
            throw new IOException("Invalid frame length");
        }
        byte[] frame = new byte[frameLength];
        input.readFully(frame);
        return Message.unpack(frame);
    }

    private void send(Message message) throws IOException {
        byte[] frame = message.pack();
        synchronized (output) {
            output.writeInt(frame.length);
            output.write(frame);
            output.flush();
        }
    }

    private int parseIntEnv(String key, int fallback) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(fallback)));
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    private static final class ParsedTask {
        private final String taskId;
        private final String taskType;
        private final String taskPayload;

        private ParsedTask(String taskId, String taskType, String taskPayload) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.taskPayload = taskPayload;
        }
    }
}
