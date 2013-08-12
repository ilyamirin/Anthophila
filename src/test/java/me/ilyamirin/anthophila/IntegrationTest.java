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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
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

        String host = "127.0.0.1";
        Integer port = 7621;

        Map<String, Object> params = Maps.newHashMap();
        params.put(Server.ServerParams.PATH_TO_STORAGE, "test.bin");
        params.put(Server.ServerParams.HOST, host);
        params.put(Server.ServerParams.PORT, port);

        Server server = new Server(params);
        Thread thread = new Thread(server);
        thread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ex) {
        }

        byte[] md5Hash = new byte[Storage.MD5_HASH_LENGTH];
        byte[] chunk = new byte[Storage.CHUNK_LENGTH];

        Client client = Client.newClient(host, port, 3, 10);

        assertTrue(client.isConnected());

        int counter = 10000;
        while (client.isConnected() && counter > 0) {
            r.nextBytes(md5Hash);
            r.nextBytes(chunk);

            assertTrue(client.push(md5Hash, chunk));
            assertTrue(Arrays.equals(chunk, client.pull(md5Hash)));

            if (r.nextBoolean()) {
                assertTrue(client.remove(md5Hash));
                assertNull(client.pull(md5Hash));
            }

            counter--;
            if (counter % 1000 == 0) {
                log.info("{} requests quads remain.", counter);
            }

        }//while

        //assertEquals(host, host);

        client.close();
    }//simpleTest
}
