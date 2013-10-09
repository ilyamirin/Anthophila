package me.ilyamirin.anthophila;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.Client;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author ilyamirin
 */
@Slf4j
public class IntegrationTest {

    private Random r = new Random();

    @Test
    public void simpleTest() throws IOException, InterruptedException {
        File file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        String host = "127.0.0.1";
        int port = 7621;

        ServerParams serverParams = new ServerParams();
        serverParams.setStorageFile("test.bin");

        serverParams.setHost(host);
        serverParams.setPort(port);

        serverParams.setEncrypt(true);
        serverParams.setNewKeysFile("new.keys");
        serverParams.setOldKeysFile("old.keys");

        serverParams.setMaxConnections(10);

        serverParams.setServeAll(true);

        FileWriter writer = new FileWriter("server.json");
        new Gson().toJson(serverParams, ServerParams.class, writer);
        writer.close();

        Server.main();

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
        final AtomicInteger chunksStored = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(clientsNumber);

        long start = System.currentTimeMillis();

        for (int i = 0; i < clientsNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
                    byte[] chunk = new byte[ServerStorage.CHUNK_LENGTH];

                    for (int j = 0; j < requestsNumber; j++) {
                        md5Hash.putLong(0, r.nextLong());
                        r.nextBytes(chunk);

                        try {
                            client.push(md5Hash.array(), chunk);
                            if (!Arrays.equals(chunk, client.pull(md5Hash.array()))) {
                                log.error("Returned result is incorrect.");
                            }
                        } catch (IOException ex) {
                            log.error("Oops!", ex);
                            errorsCounter.incrementAndGet();
                        }

                        if (r.nextBoolean()) {
                            try {
                                if (!client.remove(md5Hash.array())) {
                                    throw new IOException("Client couldn`t remove chunk.");
                                }
                                if (client.pull(md5Hash.array()) != null) {
                                    throw new IOException("Client returned previously removed chunk.");
                                }
                            } catch (IOException exception) {
                                errorsCounter.incrementAndGet();
                                log.error("Oops!", exception);
                            }
                        } else {
                            chunksStored.incrementAndGet();
                        }

                        if (requestsPassed.incrementAndGet() % 1000 == 0) {
                            log.info("{} pull/push/?remove/pull request quads passed.", requestsPassed.get());
                        }

                    }//for

                    latch.countDown();

                }//run
            }.start();

        }//while

        latch.await();

        assertEquals(0, errorsCounter.get());
        assertTrue(chunksStored.get() * ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH <= file.length());

        log.info("Test was passed for {} seconds.", (System.currentTimeMillis() - start) / 1000);

        client.close();
    }//simpleTest
}
