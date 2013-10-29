package me.ilyamirin.anthophila.server;

import com.google.common.hash.BloomFilter;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.Topology;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import me.ilyamirin.anthophila.client.ReplicationClient;
import me.ilyamirin.anthophila.common.Node;

@Slf4j
@AllArgsConstructor
public class Server extends Thread {

    public final class OperationTypes {

        public static final byte PUSHING = Byte.MAX_VALUE;
        public static final byte PULLING = Byte.MAX_VALUE - 1;
        public static final byte REMOVING = Byte.MAX_VALUE - 2;
        public static final byte SEEKING = Byte.MAX_VALUE - 3;
    }

    public final class OperationResultStatus {

        public static final byte SUCCESS = Byte.MAX_VALUE;
        public static final byte CHUNK_WAS_NOT_FOUND = Byte.MAX_VALUE - 1;
        public static final byte CHUNK_WAS_FOUND = Byte.MAX_VALUE - 2;
        public static final byte KEY_IS_OUT_OF_RANGE = Byte.MAX_VALUE - 3;
        public static final byte FAILURE = Byte.MIN_VALUE;
    }

    @NonNull
    private ServerParams params;
    @NonNull
    private ServerStorage storage;
    private Topology topology;
    @NonNull
    private BloomFilter<byte[]> bloomFilter;
    @NonNull
    private ReplicationClient replicationClient;

    @Override
    public void run() {
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {

            InetSocketAddress inetSocketAddress = new InetSocketAddress(params.getHost(), params.getPort());
            serverSocketChannel.bind(inetSocketAddress, params.getMaxConnections());
            serverSocketChannel.configureBlocking(false);

            log.info("Waiting for a client...");

            ExecutorService executor = Executors.newCachedThreadPool();
            
            while (serverSocketChannel.isOpen()) {
                SocketChannel channel = serverSocketChannel.accept();                                

                if (channel != null) {
                    String host = channel.socket().getInetAddress().getHostAddress();
                    int port = channel.socket().getPort();
                    Node node = new Node(host, port);
                    
                    boolean isAnotherNode = topology.getNodes().containsKey(node);
                    
                    log.info("{} {}", node, isAnotherNode);
                    
                    executor.execute(new ServerWorker(params, storage, topology, channel, bloomFilter, replicationClient, isAnotherNode));
                }
            }//while

        } catch (Exception x) {
            log.error("Oops!", x);

        }//try ServerSocketChannel
    }//run    

    public static void main(String... args) throws IOException {
        String pathToConfig = args.length > 0 ? args[0] : "server.json";
        ServerParams serverParams = new Gson().fromJson(new FileReader(pathToConfig), ServerParams.class);
        log.info("{}", serverParams);
        
        Topology topology = Topology.loadFromFile(serverParams.getTopologyFile());
        log.info("{}", topology);
        
        ServerEnigma serverEnigma = ServerEnigma.newServerEnigma(serverParams);
        
        ServerStorage serverStorage = ServerStorage.newServerStorage(serverParams, serverEnigma);

        BloomFilter<byte[]> bloomFilter = serverStorage.loadExistedStorage();
        
        ReplicationClient replicationClient = ReplicationClient.newReplicationClient(serverParams, topology);
        
        Server server = new Server(serverParams, serverStorage, topology, bloomFilter, replicationClient);
        server.start();
    }

}
