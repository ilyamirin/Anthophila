package me.ilyamirin.anthophila;

import me.ilyamirin.anthophila.server.Storage;
import me.ilyamirin.anthophila.server.StorageImpl;
import me.ilyamirin.anthophila.server.Server;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static junit.framework.Assert.*;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class IntegrationTest {

    private Random r = new Random();
    private Storage storage;
    private RandomAccessFile aFile;

    public void cleanStorageFile() throws IOException {
        File file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
    }

    @Test
    public void simpleTest() throws IOException {
        cleanStorageFile();

        String host = "localhost"; Integer port = 7621;

        Map<String, Object> params = Maps.newHashMap();
        params.put(Server.ServerParams.PATH_TO_STORAGE, "test.bin");
        params.put(Server.ServerParams.HOST, host);
        params.put(Server.ServerParams.PORT, port);

        Server server = new Server();
        server.start(params);

        try (Socket socket = new Socket(host, port)) {
            assertTrue(socket.isConnected());
        }

        Map<Long, ByteBuffer> hashesWithChunks = Maps.newHashMap();

        ByteBuffer md5Hash; ByteBuffer chunk;
        for (int i = 0; i < 10; i++) {
            md5Hash = ByteBuffer.allocate(8);
            r.nextBytes(md5Hash.array());

            chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH);
            r.nextBytes(chunk.array());

            hashesWithChunks.put(md5Hash.getLong(0), chunk);
        }

        Client client = new Client(host, port, 5);
        client.init();

        Map<Long, Byte> result = client.sendChunks(hashesWithChunks);

        assertEquals(result.size(), hashesWithChunks.size());
        
        for (Map.Entry<Long, Byte> entry : result.entrySet()) {
            assertTrue(hashesWithChunks.containsKey(entry.getKey()));
            assertEquals(Byte.MAX_VALUE, hashesWithChunks.get(entry.getKey()));
        }

        client.close();
        server.stop();
    }
}
