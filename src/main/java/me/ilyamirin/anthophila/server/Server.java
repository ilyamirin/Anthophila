package me.ilyamirin.anthophila.server;

import com.google.common.hash.BloomFilter;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.Topology;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import me.ilyamirin.anthophila.client.ReplicationClient;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.collections.map.MultiKeyMap;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;

@Slf4j
@AllArgsConstructor
public class Server extends Thread {

    public final class OperationTypes {

        public static final byte PUSHING = Byte.MAX_VALUE;
        public static final byte PULLING = Byte.MAX_VALUE - 1;
        public static final byte REMOVING = Byte.MAX_VALUE - 2;
        public static final byte SEEKING = Byte.MAX_VALUE - 3;
        public static final byte SET_CONNECTION_TYPE = Byte.MAX_VALUE - 4;
    }

    public final class OperationResultStatus {

        public static final byte SUCCESS = Byte.MAX_VALUE;
        public static final byte CHUNK_WAS_NOT_FOUND = Byte.MAX_VALUE - 1;
        public static final byte CHUNK_WAS_FOUND = Byte.MAX_VALUE - 2;
        public static final byte KEY_IS_OUT_OF_RANGE = Byte.MAX_VALUE - 3;
        public static final byte CHUNK_KEY_IS_INCONSISTENT = Byte.MAX_VALUE - 4;
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
                    executor.execute(new ServerWorker(params, storage, topology, channel, bloomFilter, replicationClient));
                }
            }//while

        } catch (Exception x) {
            log.error("Oops!", x);

        }//try ServerSocketChannel
    }//run    

    public static void main(String... args) throws Exception {
        String pathToConfig = args.length > 0 ? args[0] : "server.json";
        ServerParams params = new Gson().fromJson(new FileReader(pathToConfig), ServerParams.class);
        log.info("{}", params);
        
        Topology topology = Topology.loadFromFile(params.getTopologyFile());
        log.info("{}", topology);
        
        ServerEnigma serverEnigma = ServerEnigma.newServerEnigma(params);
        
        Initiator initiator = new Initiator(Configuration.create());
        initiator.createSession(params.getTarget());
        
        ServerStorage serverStorage = new ServerStorage(params, serverEnigma, MultiKeyMap.decorate(new LinkedMap(1000)), new ArrayList<ServerIndexEntry>(), initiator);

        BloomFilter<byte[]> bloomFilter = serverStorage.loadExistedStorage();
        
        ReplicationClient replicationClient = ReplicationClient.newReplicationClient(params, topology);
        
        Server server = new Server(params, serverStorage, topology, bloomFilter, replicationClient);
        server.start();
    }

}
