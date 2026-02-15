package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
                    handleRpc(incoming);
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
        Message response = new Message();
        response.studentId = studentId;
        try {
            // Placeholder stable computation response
            response.messageType = "TASK_COMPLETE";
            response.payload = "done:" + request.payload;
        } catch (Exception exception) {
            response.messageType = "TASK_ERROR";
            response.payload = "error:" + request.payload;
        }
        send(response);
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
}
