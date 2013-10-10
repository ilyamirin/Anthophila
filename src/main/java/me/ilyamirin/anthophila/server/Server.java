package me.ilyamirin.anthophila.server;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.Topology;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

@Slf4j
@AllArgsConstructor
public class Server extends Thread {

    public final class OperationTypes {

        public static final byte PUSHING = Byte.MAX_VALUE;
        public static final byte PULLING = Byte.MAX_VALUE - 1;
        public static final byte REMOVING = Byte.MAX_VALUE - 2;
    }

    public final class OperationResultStatus {

        public static final byte SUCCESS = Byte.MAX_VALUE;
        public static final byte CHUNK_WAS_NOT_FOUND = Byte.MAX_VALUE - 1;
        public static final byte KEY_IS_OUT_OF_RANGE = Byte.MAX_VALUE - 2;
        public static final byte FAILURE = Byte.MIN_VALUE;
    }

    @NonNull
    private ServerParams params;
    @NonNull
    private ServerStorage storage;
    private Topology topology;

    private static void writeResponse(SocketChannel channel, ByteBuffer response) throws IOException {
        response.rewind();
        while (response.hasRemaining())
            channel.write(response);
    }

    protected void push(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
        response.put(md5Hash.array());

        if (!params.isServeAll() && !topology.isKeyServableForServer(md5Hash, params)) {
            response.put(OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer chunkLength = ByteBuffer.allocate(4);
        while (chunkLength.hasRemaining()) {
            channel.read(chunkLength);
        }

        ByteBuffer chunk = ByteBuffer.allocate(chunkLength.getInt(0));
        while (chunk.hasRemaining()) {
            channel.read(chunk);
        }

        storage.append(md5Hash, chunk);

        response.put(OperationResultStatus.SUCCESS);

        writeResponse(channel, response);
    }

    protected void pull(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
        }

        if (!params.isServeAll() && !topology.isKeyServableForServer(md5Hash, params)) {
            ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
            response.put(md5Hash.array());
            response.put(OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer chunk = storage.read(md5Hash);

        ByteBuffer response;
        if (chunk != null) {
            response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1 + 4 + chunk.capacity());
            response.put(md5Hash.array());
            response.put(OperationResultStatus.SUCCESS);
            response.putInt(chunk.capacity());
            response.put(chunk.array());

        } else {
            response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
            response.put(md5Hash.array());
            response.put(OperationResultStatus.CHUNK_WAS_NOT_FOUND);

        }

        writeResponse(channel, response);
    }

    protected void remove(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
        }

        storage.delete(md5Hash);

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
        response.put(md5Hash.array());
        response.put(OperationResultStatus.SUCCESS);

        writeResponse(channel, response);
    }

    @Override
    public void run() {
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {

            InetSocketAddress inetSocketAddress = new InetSocketAddress(params.getHost(), params.getPort());
            serverSocketChannel.bind(inetSocketAddress, params.getMaxConnections());
            serverSocketChannel.configureBlocking(false);

            log.info("Waiting for a client...");

            while (true) {
                SocketChannel channel = serverSocketChannel.accept();
                if (channel == null) {
                    continue;
                }

                ByteBuffer typeOfOperation = ByteBuffer.allocate(1);
                while (channel.isConnected()) {
                    typeOfOperation.position(0);
                    channel.read(typeOfOperation);
                    byte operationType = typeOfOperation.get(0);

                    if (operationType == OperationTypes.PUSHING) {
                        push(channel);

                    } else if (operationType == OperationTypes.PULLING) {
                        pull(channel);

                    } else if (operationType == OperationTypes.REMOVING) {
                        remove(channel);

                    } else {
                        log.warn("Unknown operation type {}", operationType);

                    }
                }//while channel.isConnected()

            }//while

        } catch (Exception x) {
            log.error("Oops!", x);

        }//try ServerSocketChannel
    }//run

    public static void main(String... args) throws IOException {
        String pathToConfig = args.length > 0 ? args[0] : "server.json";
        ServerParams serverParams = new Gson().fromJson(new FileReader(pathToConfig), ServerParams.class);
        log.info("{}", serverParams);
        Topology topology = serverParams.isServeAll() ? null : Topology.loadFromFile(serverParams.getTopologyFile());
        ServerEnigma serverEnigma = ServerEnigma.newServerEnigma(serverParams);
        ServerStorage serverStorage = ServerStorage.newServerStorage(serverParams, serverEnigma);
        Server server = new Server(serverParams, serverStorage, topology);
        server.start();
    }

}
