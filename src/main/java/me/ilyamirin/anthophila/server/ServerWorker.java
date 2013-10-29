package me.ilyamirin.anthophila.server;

import com.google.common.hash.BloomFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.ReplicationClient;
import me.ilyamirin.anthophila.common.Topology;
import me.ilyamirin.anthophila.client.Client;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@RequiredArgsConstructor
public class ServerWorker implements Runnable {

    @NonNull
    protected ServerParams params;
    @NonNull
    protected ServerStorage storage;
    @NonNull
    protected Topology topology;
    @NonNull
    protected SocketChannel channel;
    @NonNull
    private BloomFilter<byte[]> filter;
    @NonNull
    private ReplicationClient replicationClient;
    private byte connectionType = Client.ConnectionType.OTHERS;

    protected static void writeResponse(SocketChannel channel, ByteBuffer response) throws IOException {
        response.rewind();
        while (response.hasRemaining()) {
            channel.write(response);
        }
    }

    protected void setConnectionType(SocketChannel channel) throws IOException {
        ByteBuffer type = ByteBuffer.allocate(1);
        while (type.hasRemaining()) {
            channel.read(type);
        }

        if (connectionType != type.get(0)) {
            connectionType = type.get(0);
        }

        type.put(0, Server.OperationResultStatus.SUCCESS);

        type.rewind();

        writeResponse(channel, type);
    }

    protected void push(SocketChannel channel) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        while (key.hasRemaining()) {
            channel.read(key);
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
        response.put(key.array());

        if (!params.isServeAll() && !topology.isKeyServableForServer(key, params)) {
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

        storage.append(key, chunk);

        filter.put(key.array());

        if (connectionType != Client.ConnectionType.REPLICA) {
            replicationClient.push(key, chunk);
        }

        response.put(Server.OperationResultStatus.SUCCESS);

        writeResponse(channel, response);
    }

    protected void pull(SocketChannel channel) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        while (key.hasRemaining()) {
            channel.read(key);
        }

        if (!params.isServeAll() && !topology.isKeyServableForServer(key, params)) {
            ByteBuffer response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
            response.put(key.array());
            response.put(Server.OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer chunk = filter.mightContain(key.array()) ? storage.read(key) : null;

        ByteBuffer response;
        if (chunk != null) {
            response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1 + 4 + chunk.capacity());
            response.put(key.array());
            response.put(Server.OperationResultStatus.SUCCESS);
            response.putInt(chunk.capacity());
            response.put(chunk.array());

        } else {
            response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
            response.put(key.array());
            response.put(Server.OperationResultStatus.CHUNK_WAS_NOT_FOUND);

        }

        writeResponse(channel, response);
    }

    protected void remove(SocketChannel channel) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        while (key.hasRemaining()) {
            channel.read(key);
        }

        if (filter.mightContain(key.array())) {
            storage.delete(key);
            if (connectionType != Client.ConnectionType.REPLICA) {
                replicationClient.remove(key);
            }
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
        response.put(key.array());
        response.put(Server.OperationResultStatus.SUCCESS);

        writeResponse(channel, response);
    }

    protected void seek(SocketChannel channel) throws IOException {
        ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        while (key.hasRemaining()) {
            channel.read(key);
        }

        if (!params.isServeAll() && !topology.isKeyServableForServer(key, params)) {
            ByteBuffer response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
            response.put(key.array());
            response.put(Server.OperationResultStatus.KEY_IS_OUT_OF_RANGE);
            writeResponse(channel, response);
            return;
        }

        ByteBuffer response = ByteBuffer.allocate(ServerStorage.KEY_LENGTH + 1);
        response.put(key.array());

        boolean isFound = filter.mightContain(key.array()) && storage.contains(key);
        if (connectionType != Client.ConnectionType.REPLICA) {
            isFound &= replicationClient.seek(key);
        }

        if (isFound) {
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
                } else if (operationType == Server.OperationTypes.SET_CONNECTION_TYPE) {
                    setConnectionType(channel);
                } else {
                    log.warn("Unknown operation type {}", operationType);
                }
            }
        } catch (IOException ioe) {
            log.error("Worker exception:", ioe);
        }
    }
}
