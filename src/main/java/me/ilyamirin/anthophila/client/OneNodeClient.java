package me.ilyamirin.anthophila.client;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

/**
 * @author ilyamirin
 */
@Slf4j
public class OneNodeClient implements Client {

    @Setter
    private SocketChannel socketChannel;

    public static OneNodeClient newClient(String host, int port) throws IOException {
        OneNodeClient client = new OneNodeClient();
        SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
        client.setSocketChannel(socketChannel);
        return client;
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    @Override
    public synchronized boolean push(ByteBuffer key, ByteBuffer chunk) throws IOException {
        if (!socketChannel.isConnected())
            socketChannel.connect(socketChannel.getLocalAddress());

        key.rewind(); chunk.rewind();

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH + 4 + chunk.limit());
        request.put(Server.OperationTypes.PUSHING); //push chunk operation
        request.put(key);
        request.putInt(chunk.limit());
        request.put(chunk);

        request.rewind();
        while (request.hasRemaining())
            socketChannel.write(request);

        ByteBuffer returnedKey = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (returnedKey.hasRemaining())
            socketChannel.read(returnedKey);

        key.rewind(); returnedKey.rewind();
        if (!key.equals(returnedKey))
            throw new IOException("Server returned another md5 hash.");

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining())
            socketChannel.read(resultBuffer);

        if (resultBuffer.get(0) == Server.OperationResultStatus.SUCCESS) {
            return true;
        } else {
            log.error("Server response status is not SUCCESS: {}", resultBuffer.get(0));
            return false;
        }
    }//push

    @Override
    public synchronized ByteBuffer pull(ByteBuffer key) throws IOException {
        if (!socketChannel.isConnected())
            socketChannel.connect(socketChannel.getLocalAddress());

        key.rewind();

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH);
        request.put(Server.OperationTypes.PULLING);
        request.put(key);

        request.rewind();
        while (request.hasRemaining())
            socketChannel.write(request);

        ByteBuffer returnedKey = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (returnedKey.hasRemaining())
            socketChannel.read(returnedKey);

        key.rewind(); returnedKey.rewind();
        if (!key.equals(returnedKey)) {
            log.error("Server returned another md5 hash: {} {}", key, returnedKey.array());
            throw new IOException("Server returned another md5 hash.");
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining())
            socketChannel.read(resultBuffer);

        if (resultBuffer.get(0) == Server.OperationResultStatus.CHUNK_WAS_NOT_FOUND) {
            return null;
        }

        ByteBuffer chunkLengthBuffer = ByteBuffer.allocate(4);
        while (chunkLengthBuffer.hasRemaining())
            socketChannel.read(chunkLengthBuffer);

        ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkLengthBuffer.getInt(0));
        while (chunkBuffer.hasRemaining())
            socketChannel.read(chunkBuffer);

        return chunkBuffer;

    }//pull

    @Override
    public synchronized boolean remove(ByteBuffer key) throws IOException {
        if (!socketChannel.isConnected())
            socketChannel.connect(socketChannel.getLocalAddress());

        key.rewind();

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH);
        request.put(Server.OperationTypes.REMOVING);
        request.put(key);

        request.position(0);
        while (request.hasRemaining())
            socketChannel.write(request);

        ByteBuffer returnedKey = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (returnedKey.hasRemaining())
            socketChannel.read(returnedKey);

        key.rewind(); returnedKey.rewind();
        if (!key.equals(returnedKey)) {
            log.error("Server returned another md5 hash: {} {}", key.array(), returnedKey.array());
            throw new IOException("Server returned another md5 hash.");
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining())
            socketChannel.read(resultBuffer);

        return resultBuffer.get(0) == Server.OperationResultStatus.SUCCESS;

    }//pull

    @Override
    public void close() throws IOException {
        socketChannel.finishConnect();
    }
}
