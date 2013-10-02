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
public class Client {

    @Setter
    private SocketChannel socketChannel;

    private Client() {
    }

    public static Client newClient(String host, int port) throws IOException {
        Client client = new Client();
        SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
        //socketChannel.configureBlocking(false);
        client.setSocketChannel(socketChannel);
        return client;
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public synchronized boolean push(byte[] md5Hash, byte[] chunk) throws IOException {
        if (!socketChannel.isConnected()) {
            socketChannel.connect(socketChannel.getLocalAddress());
        }

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH + 4 + chunk.length);
        request.put(Server.OperationTypes.PUSHING); //push chunk operation
        request.put(md5Hash);
        request.putInt(chunk.length);
        request.put(chunk);

        request.position(0);
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer md5HashBuffer = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5HashBuffer.hasRemaining()) {
            socketChannel.read(md5HashBuffer);
        }
        if (!Arrays.equals(md5Hash, md5HashBuffer.array())) {
            throw new IOException("Server returned another md5 hash.");
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining()) {
            socketChannel.read(resultBuffer);
        }
        if (resultBuffer.get(0) == Server.OperationResultStatus.SUCCESS) {
            return true;
        } else {
            return false;
        }
    }//push

    public synchronized byte[] pull(byte[] md5Hash) throws IOException {
        if (!socketChannel.isConnected()) {
            socketChannel.connect(socketChannel.getLocalAddress());
        }

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH);
        request.put(Server.OperationTypes.PULLING);
        request.put(md5Hash);

        request.position(0);
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer md5HashBuffer = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5HashBuffer.hasRemaining()) {
            socketChannel.read(md5HashBuffer);
        }
        if (!Arrays.equals(md5Hash, md5HashBuffer.array())) {
            log.error("Server returned another md5 hash: {} {}", md5Hash, md5HashBuffer.array());
            throw new IOException("Server returned another md5 hash.");
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining()) {
            socketChannel.read(resultBuffer);
        }
        if (resultBuffer.get(0) == Server.OperationResultStatus.CHUNK_WAS_NOT_FOUND) {
            return null;
        } else if (resultBuffer.get(0) != Server.OperationResultStatus.SUCCESS) {
            throw new IOException("Server could not retrun chunk.");
        }

        ByteBuffer chunkLengthBuffer = ByteBuffer.allocate(4);
        while (chunkLengthBuffer.hasRemaining()) {
            socketChannel.read(chunkLengthBuffer);
        }

        ByteBuffer chunkBuffer = ByteBuffer.allocate(chunkLengthBuffer.getInt(0));
        while (chunkBuffer.hasRemaining()) {
            socketChannel.read(chunkBuffer);
        }

        return chunkBuffer.array();

    }//pull

    public synchronized boolean remove(byte[] md5Hash) throws IOException {
        if (!socketChannel.isConnected()) {
            socketChannel.connect(socketChannel.getLocalAddress());
        }

        ByteBuffer request = ByteBuffer.allocate(1 + ServerStorage.MD5_HASH_LENGTH);
        request.put(Server.OperationTypes.REMOVING);
        request.put(md5Hash);

        request.position(0);
        while (request.hasRemaining()) {
            socketChannel.write(request);
        }

        ByteBuffer md5HashBuffer = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5HashBuffer.hasRemaining()) {
            socketChannel.read(md5HashBuffer);
        }
        if (!Arrays.equals(md5Hash, md5HashBuffer.array())) {
            log.error("Server returned another md5 hash: {} {}", md5Hash, md5HashBuffer.array());
            throw new IOException("Server returned another md5 hash.");
        }

        ByteBuffer resultBuffer = ByteBuffer.allocate(1);
        while (resultBuffer.hasRemaining()) {
            socketChannel.read(resultBuffer);
        }
        if (resultBuffer.get(0) == Server.OperationResultStatus.SUCCESS) {
            return true;
        } else {
            return false;
        }

    }//pull

    public void close() throws IOException {
        socketChannel.finishConnect();
    }
}
