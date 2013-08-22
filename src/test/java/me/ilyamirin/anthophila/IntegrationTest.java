package me.ilyamirin.anthophila;

import me.ilyamirin.anthophila.server.ServerStorage;
import me.ilyamirin.anthophila.server.Server;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.Client;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class IntegrationTest {

    private Random r = new Random();
    private ServerStorage storage;
    private RandomAccessFile aFile;

    public void cleanStorageFile() throws IOException {
        File file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
    }

    @Test
    public void simpleTest() throws IOException, InterruptedException {
        cleanStorageFile();

        String host = "127.0.0.1";
        Integer port = 7621;
/*
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

        final Client client = Client.newClient(host, port);

        assertTrue(client.isConnected());

        final int clientsNumber = 10;
        final int requestsNumber = 1000;
        final AtomicInteger errorsCounter = new AtomicInteger(0);
        final AtomicInteger requestsPassed = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(clientsNumber);

        for (int i = 0; i < clientsNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    byte[] md5Hash;
                    byte[] chunk;
                    for (int j = 0; j < requestsNumber; j++) {
                        md5Hash = new byte[Storage.MD5_HASH_LENGTH];
                        chunk = new byte[Storage.CHUNK_LENGTH];
                        r.nextBytes(md5Hash);
                        r.nextBytes(chunk);

                        try {
                            client.push(md5Hash, chunk);
                            if (!Arrays.equals(chunk, client.pull(md5Hash))) {
                                throw new IOException("Returned result is incorrect.");
                            }

                        } catch (IOException ex) {
                            log.error("Oops!", ex);
                            errorsCounter.incrementAndGet();
                        }

                        if (r.nextBoolean()) {
                            try {
                                if (!client.remove(md5Hash)) {
                                    throw new IOException("Client couldn`t remove chunk.");
                                }
                                if (client.pull(md5Hash) != null) {
                                    throw new IOException("Client returned previously removed chunk.");
                                }
                            } catch (IOException exception) {
                                errorsCounter.incrementAndGet();
                                log.error("Oops!", exception);
                            }
                        }

                        if (requestsPassed.incrementAndGet() % 1000 == 0) {
                            log.info("{} requests quads passed.", requestsPassed.get());
                        }

                    }//for

                    latch.countDown();

                }//run
            }.start();

        }//while

        latch.await();

        assertEquals(0, errorsCounter.get());

        client.close();*/
    }//simpleTest
}
