package com.example.family;

import family.*;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class NodeMain {

    private static final int START_PORT = 5555;
    private static final int PRINT_INTERVAL_SECONDS = 10;

    // Mesaj deposu - disk tabanlı
    private static final MessageStore messageStore = new MessageStore();

    // 4. Aşama - Tolerance config ve message registry
    private static ToleranceConfig toleranceConfig;
    private static MessageRegistry messageRegistry;

    // Round-robin için index
    private static int roundRobinIndex = 0;

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = findFreePort(START_PORT);

        NodeInfo self = NodeInfo.newBuilder()
                .setHost(host)
                .setPort(port)
                .build();

        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);
        StorageServiceImpl storageService = new StorageServiceImpl(messageStore);

        // 4. Aşama - Tolerance ve message registry başlat
        toleranceConfig = new ToleranceConfig();
        messageRegistry = new MessageRegistry();

        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .addService(storageService)
                .build()
                .start();

        System.out.printf("Node started on %s:%d%n", host, port);

        // Eğer bu ilk node ise (port 5555), TCP 6666'da text dinlesin
        if (port == START_PORT) {
            startLeaderTextListener(registry, self);
            startStatusPrinter(); // Lider mesaj durumunu yazdırsın
        }

        discoverExistingNodes(host, port, registry, self);
        startFamilyPrinter(registry, self);
        startHealthChecker(registry, self);

        server.awaitTermination();
    }

    private static void startLeaderTextListener(NodeRegistry registry, NodeInfo self) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(6666)) {
                System.out.printf("Leader listening for text on TCP %s:%d%n", self.getHost(), 6666);

                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleClientTextConnection(client, registry, self)).start();
                }

            } catch (IOException e) {
                System.err.println("Error in leader text listener: " + e.getMessage());
            }
        }, "LeaderTextListener").start();
    }

    private static void handleClientTextConnection(Socket client,
            NodeRegistry registry,
            NodeInfo self) {
        System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter writer = new PrintWriter(client.getOutputStream(), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                String text = line.trim();
                if (text.isEmpty())
                    continue;

                System.out.println("📝 Received from TCP: " + text);
                Command cmd = Command.parse(text);

                switch (cmd.getType()) {
                    case SET:
                        handleSet(cmd, registry, self, writer);
                        break;

                    case GET:
                        handleGet(cmd, registry, self, writer);
                        break;

                    case UNKNOWN:
                    default:
                        writer.println("ERROR: Unknown command. Use SET <id> <message> or GET <id>");
                        System.out.println("❓ Unknown command: " + text);
                        break;
                }
            }

        } catch (IOException e) {
            System.err.println("TCP client handler error: " + e.getMessage());
        } finally {
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 4. Aşama - SET komutu işleme
     * 1. Lider kendine kaydeder
     * 2. Tolerance sayısı kadar üyeye gRPC ile gönderir
     * 3. Hangi üyelerde saklandığını kaydeder
     */
    private static void handleSet(Command cmd, NodeRegistry registry, NodeInfo self, PrintWriter writer) {
        int messageId = cmd.getMessageId();
        String messageText = cmd.getMessageText();
        int tolerance = toleranceConfig.getTolerance();

        // Üyeleri al (kendimiz hariç)
        List<NodeInfo> allMembers = registry.snapshot();
        List<NodeInfo> otherMembers = new ArrayList<>();
        for (NodeInfo n : allMembers) {
            if (!(n.getHost().equals(self.getHost()) && n.getPort() == self.getPort())) {
                otherMembers.add(n);
            }
        }

        // Tolerance sayısı kadar üye seç (round-robin)
        List<NodeInfo> selectedMembers = selectMembers(otherMembers, tolerance);

        // Seçilen üyelere gRPC ile gönder
        List<NodeInfo> successfulMembers = new ArrayList<>();
        for (NodeInfo member : selectedMembers) {
            if (sendStoreToMember(member, messageId, messageText)) {
                successfulMembers.add(member);
            }
        }

        // En az 1 üyeye gönderildiyse veya hiç üye yoksa, lider de kaydeder
        if (successfulMembers.size() >= 1 || otherMembers.isEmpty()) {
            messageStore.set(messageId, messageText);

            // Hangi üyelerde saklandığını kaydet
            List<NodeInfo> allStoredAt = new ArrayList<>(successfulMembers);
            allStoredAt.add(self); // Lider de sakladı
            messageRegistry.registerMessage(messageId, allStoredAt);

            writer.println("OK");
            System.out.println("✅ SET successful: id=" + messageId +
                    ", replicated to " + successfulMembers.size() + " members");
        } else {
            writer.println("ERROR: Could not replicate to enough members");
            System.out.println("❌ SET failed: id=" + messageId + ", replication failed");
        }
    }

    /**
     * Round-robin ile tolerance sayısı kadar üye seç
     */
    private static List<NodeInfo> selectMembers(List<NodeInfo> members, int count) {
        List<NodeInfo> selected = new ArrayList<>();
        if (members.isEmpty())
            return selected;

        int toSelect = Math.min(count, members.size());
        for (int i = 0; i < toSelect; i++) {
            int index = (roundRobinIndex + i) % members.size();
            selected.add(members.get(index));
        }
        roundRobinIndex = (roundRobinIndex + toSelect) % Math.max(1, members.size());

        return selected;
    }

    /**
     * Bir üyeye gRPC Store isteği gönder
     */
    private static boolean sendStoreToMember(NodeInfo member, int messageId, String text) {
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(member.getHost(), member.getPort())
                    .usePlaintext()
                    .build();

            StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

            StoredMessage msg = StoredMessage.newBuilder()
                    .setId(messageId)
                    .setText(text)
                    .build();

            StoreResult result = stub.store(msg);

            if (result.getSuccess()) {
                System.out.printf("📤 Replicated id=%d to %s:%d%n",
                        messageId, member.getHost(), member.getPort());
                return true;
            } else {
                System.err.printf("Store failed at %s:%d: %s%n",
                        member.getHost(), member.getPort(), result.getError());
                return false;
            }

        } catch (Exception e) {
            System.err.printf("Failed to store at %s:%d: %s%n",
                    member.getHost(), member.getPort(), e.getMessage());
            return false;
        } finally {
            if (channel != null)
                channel.shutdownNow();
        }
    }

    /**
     * 4. Aşama - GET komutu işleme
     * 1. Önce liderin diskinde ara
     * 2. Yoksa mesajın tutulduğu üyelerden gRPC ile al
     */
    private static void handleGet(Command cmd, NodeRegistry registry, NodeInfo self, PrintWriter writer) {
        int messageId = cmd.getMessageId();

        // Önce kendi diskinde ara
        String message = messageStore.get(messageId);
        if (message != null) {
            writer.println(message);
            System.out.println("✅ GET successful (local): id=" + messageId);
            return;
        }

        // Yoksa üyelerden al
        List<NodeInfo> members = messageRegistry.getMembers(messageId);
        for (NodeInfo member : members) {
            // Kendimizi atla (zaten yukarıda baktık)
            if (member.getHost().equals(self.getHost()) && member.getPort() == self.getPort()) {
                continue;
            }

            String retrieved = retrieveFromMember(member, messageId);
            if (retrieved != null && !retrieved.isEmpty()) {
                writer.println(retrieved);
                System.out.println(
                        "✅ GET successful (from " + member.getHost() + ":" + member.getPort() + "): id=" + messageId);
                return;
            }
        }

        writer.println("NOT_FOUND");
        System.out.println("❌ GET failed: id=" + messageId + " not found");
    }

    /**
     * Bir üyeden gRPC Retrieve isteği ile mesaj al
     */
    private static String retrieveFromMember(NodeInfo member, int messageId) {
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(member.getHost(), member.getPort())
                    .usePlaintext()
                    .build();

            StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(channel);

            MessageId id = MessageId.newBuilder().setId(messageId).build();
            StoredMessage result = stub.retrieve(id);

            return result.getText();

        } catch (Exception e) {
            System.err.printf("Failed to retrieve from %s:%d: %s%n",
                    member.getHost(), member.getPort(), e.getMessage());
            return null;
        } finally {
            if (channel != null)
                channel.shutdownNow();
        }
    }

    /**
     * Lider periyodik olarak mesaj durumunu yazdırır
     */
    private static void startStatusPrinter() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("--------------------------------------");
            System.out.println("📊 Leader Status - " + LocalDateTime.now());
            System.out.println("   Local messages: " + messageStore.size());
            messageRegistry.printStatus();
            System.out.println("--------------------------------------");
        }, 15, 30, TimeUnit.SECONDS);
    }

    private static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                port++;
            }
        }
    }

    private static void discoverExistingNodes(String host, int selfPort,
            NodeRegistry registry, NodeInfo self) {
        for (int port = START_PORT; port < selfPort; port++) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);

                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());

                System.out.printf("Joined through %s:%d, family size now: %d%n",
                        host, port, registry.snapshot().size());

            } catch (Exception ignored) {
            } finally {
                if (channel != null)
                    channel.shutdownNow();
            }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            System.out.println("======================================");
            System.out.printf("Family at %s:%d (me)%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());
            System.out.println("Members:");

            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n", n.getHost(), n.getPort(), isMe ? " (me)" : "");
            }
            System.out.println("======================================");
        }, 3, PRINT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();

            for (NodeInfo n : members) {
                if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
                    continue;
                }

                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder
                            .forAddress(n.getHost(), n.getPort())
                            .usePlaintext()
                            .build();

                    FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                    stub.getFamily(Empty.newBuilder().build());

                } catch (Exception e) {
                    System.out.printf("Node %s:%d unreachable, removing from family%n",
                            n.getHost(), n.getPort());
                    registry.remove(n);
                } finally {
                    if (channel != null)
                        channel.shutdownNow();
                }
            }

        }, 5, 10, TimeUnit.SECONDS);
    }
}
