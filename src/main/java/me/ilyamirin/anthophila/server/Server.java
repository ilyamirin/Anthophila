package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class Server implements Runnable {

    public static final String CURRENT_PROTOCOL_VERSION = "/v1";

    public class ServerParams {

        public static final String PATH_TO_STORAGE = "--pathToStorage";
        public static final String HOST = "--host";
        public static final String PORT = "--port";
    }

    public class OperationTypes {

        public static final byte PUSHING = Byte.MAX_VALUE;
        public static final byte PULLING = Byte.MAX_VALUE - 1;
    }

    public class OperationResultStatus {

        public static final byte SUCCESS = Byte.MAX_VALUE;
    }
    private ServerSocketChannel serverSocketChannel;
    private Storage storage;
    private ByteBuffer typeOfOperation = ByteBuffer.allocate(1);
    private ByteBuffer md5Hash = ByteBuffer.allocate(Storage.MD5_HASH_LENGTH);
    private ByteBuffer chunkLength = ByteBuffer.allocate(4);
    private ByteBuffer chunk;
    private ByteBuffer response;
    @NonNull
    private Map<String, Object> params;

    protected void push(SocketChannel channel) throws IOException {
        log.info("Start chunk pushing operation.");

        md5Hash.position(0);
        channel.read(md5Hash);

        chunkLength.position(0);
        channel.read(chunkLength);

        chunk = ByteBuffer.allocate(chunkLength.getInt(0));
        channel.read(chunk);

        storage.append(md5Hash, chunk);

        response = ByteBuffer.allocate(Storage.MD5_HASH_LENGTH + 1);
        response.position(0);
        response.put(md5Hash.array());
        response.put(OperationResultStatus.SUCCESS);

        response.position(0);
        channel.write(response);
    }

    protected void pull(SocketChannel channel) throws IOException {
        log.info("Start chunk pulling operation.");

        md5Hash.position(0);
        channel.read(md5Hash);

        chunk = storage.read(md5Hash);

        response = ByteBuffer.allocate(Storage.MD5_HASH_LENGTH + 1 + 4 + chunk.capacity());
        response.position(0);
        response.put(md5Hash.array());
        response.put(OperationResultStatus.SUCCESS);
        response.putInt(chunk.capacity());
        response.put(chunk.array());

        response.position(0);
        channel.write(response);
    }

    @Override
    public void run() {
        try {
            String pathToStorage = (String) params.get(ServerParams.PATH_TO_STORAGE);
            RandomAccessFile accessFile = new RandomAccessFile(pathToStorage, "rw");
            storage = new StorageImpl(accessFile.getChannel());

            String host = (String) params.get(ServerParams.HOST);
            Integer port = (Integer) params.get(ServerParams.PORT);

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(host, port), 10);
            //serverSocketChannel.configureBlocking(false);

            log.info("Waiting for a client...");

            while (true) {
                SocketChannel channel = serverSocketChannel.accept();
                if (channel == null) {
                    continue;
                }

                while (channel.isConnected()) {
                    typeOfOperation.position(0);
                    channel.read(typeOfOperation);
                    byte operationType = typeOfOperation.get(0);

                    if (operationType == OperationTypes.PUSHING) {
                        push(channel);

                    } else if (operationType == OperationTypes.PULLING) {
                        pull(channel);

                    } else {
                    }
                }//while

            }//while

        } catch (Exception x) {
            log.error("Oops!", x);
        } finally {
            try {
                serverSocketChannel.close();
            } catch (IOException ex) {
            }
        }
    }

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i += 2) {
            String[] param = Arrays.copyOfRange(args, i, i + 2);
            if (param[0].equals("--pathToStorage")) {
                //accessFile
            }
        }

    }
}
