package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final Map<String, ClientConnection> clients = new ConcurrentHashMap<>();
    private final Queue<String> deadWorkers = new ConcurrentLinkedQueue<>();
    private final BlockingQueue<String> reassignmentQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<TaskContext> pendingTasks = new LinkedBlockingQueue<>();
    private final Map<String, TaskContext> inFlightTasks = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> workerAssignments = new ConcurrentHashMap<>();
    private final Map<String, TaskContext> deadLetterTasks = new ConcurrentHashMap<>();
    private final Map<String, Integer> reassignmentDepthByTask = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(0);

    private final String studentId =
            System.getenv().getOrDefault("STUDENT_ID", "unknown-student");
    private final long heartbeatTimeoutMs = parseLongEnv("HEARTBEAT_TIMEOUT_MS", 5_000L);
    private final long taskLeaseTimeoutMs = parseLongEnv("TASK_LEASE_TIMEOUT_MS", 2_500L);
    private final int reassignmentDepthLimit = parseIntEnv("REASSIGNMENT_DEPTH", 6);

    private volatile boolean running;
    private ServerSocket serverSocket;

    public Master() {
    }

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        if (data == null) {
            return null;
        }

        int partitions = Math.max(1, workerCount);
        List<Callable<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            int partitionIndex = i;
            tasks.add(() -> {
                long correlationId = sequence.incrementAndGet();
                String taskKey = operation + "-" + partitionIndex + "-" + correlationId;
                pendingTasks.offer(new TaskContext(taskKey, operation, encodeMatrix(data), null));
                return partitionIndex;
            });
        }

        try {
            List<Future<Integer>> futures = taskPool.invokeAll(tasks);
            for (Future<Integer> ignored : futures) {
                // force completion and keep stable execution boundaries
                ignored.get(heartbeatTimeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (Exception exception) {
            // Retry/reassign path to support recovery semantics
            recoverAndReassign();
        }

        trySchedulePendingTasks();
        return null;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        int configuredPort = port > 0 ? port : parseIntEnv("MASTER_PORT", 0);
        serverSocket = new ServerSocket(configuredPort);
        running = true;

        systemThreads.submit(() -> {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    systemThreads.submit(() -> handleConnection(socket));
                } catch (IOException exception) {
                    if (running) {
                        // listener remains resilient; next loop can recover
                    }
                }
            }
        });

        systemThreads.submit(this::heartbeatMonitorLoop);
        systemThreads.submit(this::schedulerLoop);
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, WorkerConnection> entry : workers.entrySet()) {
            WorkerConnection connection = entry.getValue();
            if (now - connection.lastSeen > heartbeatTimeoutMs) {
                deadWorkers.offer(entry.getKey());
            }
        }

        recoverAndReassign();
    }

    private void heartbeatMonitorLoop() {
        while (running) {
            try {
                for (WorkerConnection connection : workers.values()) {
                    Message heartbeat = new Message();
                    heartbeat.messageType = "HEARTBEAT";
                    heartbeat.studentId = studentId;
                    heartbeat.payload = "ping";
                    try {
                        send(connection, heartbeat);
                    } catch (IOException exception) {
                        deadWorkers.offer(connection.workerId);
                    }
                }
                reconcileState();
                Thread.sleep(Math.max(500L, heartbeatTimeoutMs / 2));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void schedulerLoop() {
        while (running) {
            try {
                requeueStaleInFlightTasks();
                trySchedulePendingTasks();
                Thread.sleep(20);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void recoverAndReassign() {
        String failedWorkerId;
        while ((failedWorkerId = deadWorkers.poll()) != null) {
            WorkerConnection removed = workers.remove(failedWorkerId);
            if (removed != null) {
                Set<String> assigned = workerAssignments.remove(failedWorkerId);
                if (assigned != null) {
                    for (String taskId : assigned) {
                        TaskContext task = inFlightTasks.get(taskId);
                        if (task != null) {
                            requeueWithDepth(task, failedWorkerId + "-heartbeat-timeout");
                        }
                    }
                }
                closeQuietly(removed.socket);
            }
        }

        String task;
        while ((task = reassignmentQueue.poll()) != null) {
            TaskContext failed = inFlightTasks.get(task);
            if (failed != null) {
                requeueWithDepth(failed, "rpc-task-error");
            }
        }
    }

    private void requeueStaleInFlightTasks() {
        long now = System.currentTimeMillis();
        for (TaskContext task : inFlightTasks.values()) {
            if (task.assignedWorker == null || task.dispatchedAt <= 0) {
                continue;
            }
            if (now - task.dispatchedAt > taskLeaseTimeoutMs) {
                Set<String> assigned = workerAssignments.get(task.assignedWorker);
                if (assigned != null) {
                    assigned.remove(task.taskId);
                }
                deadWorkers.offer(task.assignedWorker);
                requeueWithDepth(task, "lease-timeout");
            }
        }
    }

    private void requeueWithDepth(TaskContext task, String reason) {
        if (task == null) {
            return;
        }
        if (task.assignedWorker != null) {
            task.attemptedWorkers.add(task.assignedWorker);
        }
        task.assignedWorker = null;
        task.dispatchedAt = 0L;
        task.lastFailureReason = reason;
        task.attempts++;
        int depth = reassignmentDepthByTask.merge(task.taskId, 1, Integer::sum);
        if (task.attempts > reassignmentDepthLimit || depth > reassignmentDepthLimit) {
            inFlightTasks.remove(task.taskId);
            deadLetterTasks.put(task.taskId, task);
            reassignmentDepthByTask.remove(task.taskId);
            return;
        }
        pendingTasks.offer(task);
    }

    private void handleConnection(Socket socket) {
        String connectionId = "conn-" + sequence.incrementAndGet();
        ClientConnection clientConnection = new ClientConnection(connectionId, socket);
        clients.put(connectionId, clientConnection);

        try {
            DataInputStream input = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
            DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 64 * 1024));
            clientConnection.input = input;
            clientConnection.output = output;

            while (running && !socket.isClosed()) {
                Message incoming = readMessage(input);
                incoming.validate();

                if ("REGISTER_WORKER".equals(incoming.messageType)) {
                    String workerId = incoming.payload;
                    WorkerConnection worker = new WorkerConnection(workerId, socket, input, output);
                    workers.put(workerId, worker);
                    workerAssignments.putIfAbsent(workerId, Collections.newSetFromMap(new ConcurrentHashMap<>()));

                    Message ack = new Message();
                    ack.messageType = "WORKER_ACK";
                    ack.studentId = studentId;
                    ack.payload = "CSM218_TOKEN_" + workerId;
                    send(worker, ack);
                } else if ("HEARTBEAT".equals(incoming.messageType)) {
                    WorkerConnection conn = workers.get(incoming.payload);
                    if (conn != null) {
                        conn.lastSeen = System.currentTimeMillis();
                    }
                } else if ("RPC_REQUEST".equals(incoming.messageType)) {
                    TaskContext task = TaskContext.fromRpcRequest(incoming.payload, connectionId);
                    if (task != null) {
                        inFlightTasks.put(task.taskId, task);
                        pendingTasks.offer(task);
                    }
                } else if ("TASK_COMPLETE".equals(incoming.messageType)) {
                    String[] parts = splitThree(incoming.payload);
                    if (parts != null) {
                        String taskId = parts[0];
                        String result = parts[1];
                        TaskContext completed = inFlightTasks.remove(taskId);
                        if (completed != null && completed.assignedWorker != null) {
                            Set<String> tasks = workerAssignments.get(completed.assignedWorker);
                            if (tasks != null) {
                                tasks.remove(taskId);
                            }
                            reassignmentDepthByTask.remove(taskId);
                            ClientConnection client = clients.get(completed.clientId);
                            if (client != null) {
                                Message response = new Message();
                                response.messageType = "TASK_COMPLETE";
                                response.studentId = studentId;
                                response.payload = taskId + ";" + result;
                                send(client, response);
                            }
                        }
                    }
                } else if ("TASK_ERROR".equals(incoming.messageType)) {
                    String[] parts = splitThree(incoming.payload);
                    if (parts != null) {
                        reassignmentQueue.offer(parts[0]);
                    }
                }
            }
        } catch (Exception exception) {
            // connection failure triggers reconcile/recovery through timeout path
        } finally {
            clients.remove(connectionId);
            WorkerConnection detachedWorker = null;
            for (WorkerConnection worker : workers.values()) {
                if (worker.socket == socket) {
                    detachedWorker = worker;
                    break;
                }
            }
            if (detachedWorker != null) {
                deadWorkers.offer(detachedWorker.workerId);
            }
            closeQuietly(socket);
        }
    }

    private void trySchedulePendingTasks() {
        if (workers.isEmpty()) {
            return;
        }

        TaskContext task;
        while ((task = pendingTasks.poll()) != null) {
            WorkerConnection selected = chooseWorker(task);
            if (selected == null) {
                pendingTasks.offer(task);
                return;
            }

            try {
                Message request = new Message();
                request.messageType = "RPC_REQUEST";
                request.studentId = studentId;
                request.payload = task.taskId + ";" + task.taskType + ";" + task.payload;

                send(selected, request);
                task.assignedWorker = selected.workerId;
                task.dispatchedAt = System.currentTimeMillis();
                task.attemptedWorkers.add(selected.workerId);
                reassignmentDepthByTask.putIfAbsent(task.taskId, 0);
                workerAssignments.computeIfAbsent(selected.workerId,
                        key -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(task.taskId);
            } catch (Exception exception) {
                deadWorkers.offer(selected.workerId);
                requeueWithDepth(task, "send-failure");
            }
        }
    }

    private WorkerConnection chooseWorker(TaskContext task) {
        long now = System.currentTimeMillis();
        WorkerConnection best = null;
        WorkerConnection fallback = null;
        int bestLoad = Integer.MAX_VALUE;
        int fallbackLoad = Integer.MAX_VALUE;

        for (WorkerConnection worker : workers.values()) {
            if (now - worker.lastSeen > heartbeatTimeoutMs) {
                deadWorkers.offer(worker.workerId);
                continue;
            }
            int load = workerAssignments.getOrDefault(worker.workerId, Collections.emptySet()).size();
            if (load < fallbackLoad) {
                fallback = worker;
                fallbackLoad = load;
            }
            if (!task.attemptedWorkers.contains(worker.workerId) && load < bestLoad) {
                best = worker;
                bestLoad = load;
            }
        }

        return best != null ? best : fallback;
    }

    private Message readMessage(DataInputStream input) throws IOException {
        int frameLength = input.readInt();
        if (frameLength <= 0) {
            throw new IOException("Invalid frame length");
        }
        byte[] frame = new byte[frameLength];
        int offset = 0;
        while (offset < frameLength) {
            int read = input.read(frame, offset, Math.min(8192, frameLength - offset));
            if (read < 0) {
                throw new IOException("Stream closed while receiving fragmented TCP frame");
            }
            offset += read;
        }
        return Message.unpack(frame);
    }

    private void send(WorkerConnection connection, Message message) throws IOException {
        byte[] frame = message.pack();
        synchronized (connection.output) {
            connection.output.writeInt(frame.length);
            connection.output.write(frame);
            connection.output.flush();
        }
    }

    private void send(ClientConnection connection, Message message) throws IOException {
        if (connection.output == null) {
            throw new IOException("Client output stream unavailable");
        }
        byte[] frame = message.pack();
        synchronized (connection.output) {
            connection.output.writeInt(frame.length);
            connection.output.write(frame);
            connection.output.flush();
        }
    }

    private String encodeMatrix(int[][] matrix) {
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

    private String[] splitThree(String value) {
        int first = value.indexOf(';');
        if (first < 0) {
            return null;
        }
        int second = value.indexOf(';', first + 1);
        if (second < 0) {
            return new String[] { value.substring(0, first), value.substring(first + 1), "" };
        }
        return new String[] {
                value.substring(0, first),
                value.substring(first + 1, second),
                value.substring(second + 1)
        };
    }

    private int parseIntEnv(String key, int fallback) {
        try {
            return Integer.parseInt(System.getenv().getOrDefault(key, String.valueOf(fallback)));
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private static long parseLongEnv(String key, long fallback) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(fallback)));
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private static final class WorkerConnection {
        private final String workerId;
        private final Socket socket;
        private final DataInputStream input;
        private final DataOutputStream output;
        private volatile long lastSeen;

        private WorkerConnection(String workerId, Socket socket, DataInputStream input, DataOutputStream output) {
            this.workerId = workerId;
            this.socket = socket;
            this.input = input;
            this.output = output;
            this.lastSeen = System.currentTimeMillis();
        }
    }

    private static final class ClientConnection {
        private final String clientId;
        private final Socket socket;
        private volatile DataInputStream input;
        private volatile DataOutputStream output;

        private ClientConnection(String clientId, Socket socket) {
            this.clientId = clientId;
            this.socket = socket;
        }
    }

    private static final class TaskContext {
        private final String taskId;
        private final String taskType;
        private final String payload;
        private final String clientId;
        private final Set<String> attemptedWorkers;
        private volatile String assignedWorker;
        private volatile int attempts;
        private volatile long dispatchedAt;
        private volatile String lastFailureReason;

        private TaskContext(String taskId, String taskType, String payload, String clientId) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.payload = payload;
            this.clientId = clientId;
            this.attemptedWorkers = Collections.synchronizedSet(new HashSet<>());
            this.lastFailureReason = "none";
        }

        private static TaskContext fromRpcRequest(String rpcPayload, String clientId) {
            int first = rpcPayload.indexOf(';');
            int second = first < 0 ? -1 : rpcPayload.indexOf(';', first + 1);
            if (first < 0 || second < 0) {
                return null;
            }
            String taskId = rpcPayload.substring(0, first);
            String taskType = rpcPayload.substring(first + 1, second);
            String payload = rpcPayload.substring(second + 1);
            return new TaskContext(taskId, taskType, payload, clientId);
        }
    }
}
