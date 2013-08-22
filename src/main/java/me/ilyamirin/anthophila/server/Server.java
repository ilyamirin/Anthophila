package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Server extends Thread {

    public final class OperationTypes {

        public static final byte PUSHING = Byte.MAX_VALUE;
        public static final byte PULLING = Byte.MAX_VALUE - 1;
        public static final byte REMOVING = Byte.MAX_VALUE - 2;
    }

    public final class OperationResultStatus {

        public static final byte SUCCESS = Byte.MAX_VALUE;
        public static final byte CHUNK_WAS_NOT_FOUND = Byte.MAX_VALUE - 1;
        public static final byte FAILURE = Byte.MIN_VALUE;
    }
    @NonNull
    private ServerStorage storage;
    @NonNull
    private ServerParams params;

    protected void push(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
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

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
        response.put(md5Hash.array());
        response.put(OperationResultStatus.SUCCESS);

        response.position(0);
        while (response.hasRemaining()) {
            channel.write(response);
        }
    }

    protected void pull(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
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

        response.position(0);
        while (response.hasRemaining()) {
            channel.write(response);
        }
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

        response.position(0);
        while (response.hasRemaining()) {
            channel.write(response);
        }
    }

    @Override
    public void run() {
        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            
            InetSocketAddress inetSocketAddress = new InetSocketAddress(params.getHost(), params.getPort());
            serverSocketChannel.bind(inetSocketAddress, params.getMaxPendingConnections());
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
                        throw new Exception("Unknown operation type " + operationType);

                    }
                }//while channel.isConnected()

            }//while

        } catch (Exception x) {
            log.error("Oops!", x);

        }//try ServerSocketChannel
    }//run
}
