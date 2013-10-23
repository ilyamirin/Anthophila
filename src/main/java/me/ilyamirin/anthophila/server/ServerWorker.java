package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.Topology;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@AllArgsConstructor
public class ServerWorker implements Runnable {

    @NonNull
    protected ServerParams params;
    @NonNull
    protected ServerStorage storage;
    protected Topology topology;
    @NonNull
    protected SocketChannel channel;

    protected static void writeResponse(SocketChannel channel, ByteBuffer response) throws IOException {
        response.rewind();
        while (response.hasRemaining()) {
            channel.write(response);
        }
    }

    protected void push(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
        response.put(md5Hash.array());

        if (!params.isServeAll() && !topology.isKeyServableForServer(md5Hash, params)) {
            response.put(md5Hash.array());
            response.put(Server.OperationResultStatus.KEY_IS_OUT_OF_RANGE);
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

        response.put(Server.OperationResultStatus.SUCCESS);

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
            response.put(Server.OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer chunk = storage.read(md5Hash);

        ByteBuffer response;
        if (chunk != null) {
            response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1 + 4 + chunk.capacity());
            response.put(md5Hash.array());
            response.put(Server.OperationResultStatus.SUCCESS);
            response.putInt(chunk.capacity());
            response.put(chunk.array());

        } else {
            response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
            response.put(md5Hash.array());
            response.put(Server.OperationResultStatus.CHUNK_WAS_NOT_FOUND);

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
        response.put(Server.OperationResultStatus.SUCCESS);

        writeResponse(channel, response);
    }
    
    protected void seek(SocketChannel channel) throws IOException {
        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        while (md5Hash.hasRemaining()) {
            channel.read(md5Hash);
        }

        if (!params.isServeAll() && !topology.isKeyServableForServer(md5Hash, params)) {
            ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
            response.put(md5Hash.array());
            response.put(Server.OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH + 1);
        response.put(md5Hash.array());
        if (storage.contains(md5Hash)) {
            response.put(Server.OperationResultStatus.CHUNK_WAS_FOUND);
        } else {
            response.put(Server.OperationResultStatus.CHUNK_WAS_NOT_FOUND);
        }

        writeResponse(channel, response);
    }
    
    @Override
    public void run() {
        try {
            ByteBuffer typeOfOperation = ByteBuffer.allocate(1);
            
            while (channel.isConnected()) {
                typeOfOperation.rewind();
                channel.read(typeOfOperation);
                byte operationType = typeOfOperation.get(0);

                if (operationType == Server.OperationTypes.PUSHING) {
                    push(channel);
                } else if (operationType == Server.OperationTypes.PULLING) {
                    pull(channel);
                } else if (operationType == Server.OperationTypes.REMOVING) {
                    remove(channel);
                } else if (operationType == Server.OperationTypes.SEEKING) {
                    seek(channel);                    
                } else {
                    log.warn("Unknown operation type {}", operationType);
                }
            }
        } catch (IOException ioe) {
            log.error("Worker exception:", ioe);
        }
    }

}
