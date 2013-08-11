package me.ilyamirin.anthophila.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.Storage;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class Client {

    @Setter
    private SocketChannel socketChannel;
    private ByteBuffer md5HashBuffer = ByteBuffer.allocate(Storage.MD5_HASH_LENGTH);
    private ByteBuffer chunkLengthBuffer = ByteBuffer.allocate(4);
    private ByteBuffer resultBuffer = ByteBuffer.allocate(1);
    private ByteBuffer chunkBuffer;
    private ByteBuffer request;

    private Client() {
    }

    public static Client newClient(String host, int port, long timeoutInSeconds) throws IOException {
        Client client = new Client();
        client.setSocketChannel(SocketChannel.open(new InetSocketAddress(host, port)));
        //socketChannel.configureBlocking(false);
        return client;
    }

    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    public boolean push(byte[] md5Hash, byte[] chunk) throws IOException {
        if (!socketChannel.isConnected()) {
            socketChannel.connect(socketChannel.getLocalAddress());
        }

        request = ByteBuffer.allocate(1 + Storage.MD5_HASH_LENGTH + 4 + chunk.length);
        request.position(0);

        request.put(Server.OperationTypes.PUSHING); //push chunk operation
        request.put(md5Hash);
        request.putInt(chunk.length);
        request.put(chunk);
        request.position(0);

        socketChannel.write(request);

        md5HashBuffer.position(0); resultBuffer.position(0);

        socketChannel.read(md5HashBuffer);
        if (!Arrays.equals(md5Hash, md5HashBuffer.array())) {
            throw new IOException("Server returned another md5 hash.");
        }

        socketChannel.read(resultBuffer);

        if (resultBuffer.get(0) == Server.OperationResultStatus.SUCCESS) {
            return true;
        } else {
            return false;
        }
    }//push

    public byte[] pull(byte[] md5Hash) throws IOException {
        if (!socketChannel.isConnected()) {
            socketChannel.connect(socketChannel.getLocalAddress());
        }

        request = ByteBuffer.allocate(1 + Storage.MD5_HASH_LENGTH);
        request.position(0);

        request.put(Server.OperationTypes.PULLING); //pull chunk operation
        request.put(md5Hash);
        request.position(0);

        socketChannel.write(request);

        md5HashBuffer.position(0);
        socketChannel.read(md5HashBuffer);
        if (!Arrays.equals(md5Hash, md5HashBuffer.array())) {
            log.error("Server returned another md5 hash: {} {}", md5Hash, md5HashBuffer.array());
            throw new IOException("Server returned another md5 hash.");
        }

        resultBuffer.position(0);
        socketChannel.read(resultBuffer);
        if (resultBuffer.get(0) != Server.OperationResultStatus.SUCCESS) {
            throw new IOException("Server did not resieve chunk.");
        }

        chunkLengthBuffer.position(0);
        socketChannel.read(chunkLengthBuffer);

        chunkBuffer = ByteBuffer.allocate(chunkLengthBuffer.getInt(0));
        chunkBuffer.position(0);
        socketChannel.read(chunkBuffer);

        return chunkBuffer.array();

    }//pull

    public void close() throws IOException {
        socketChannel.finishConnect();
    }
}
