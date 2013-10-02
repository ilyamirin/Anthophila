package me.ilyamirin.anthophila;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.Client;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerEncryptor;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
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

    //TODO:: add encryption on, off, and on again
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
        serverParams.setEncrypted(true);
        serverParams.setHost(host);
        serverParams.setPort(port);
        serverParams.setMaxPendingConnections(10);
        serverParams.setPathToStorageFile(file.getAbsolutePath());

        Set<String> newKeysSet = Sets.newHashSet(ServerEncryptor.generateKeys(64).values());
        Set<String> oldKeysSet = Sets.newHashSet();
        ServerEncryptor serverEncryptor = ServerEncryptor.newServerEncryptor(newKeysSet, oldKeysSet);

        ServerStorage serverStorage = ServerStorage.newServerStorage(serverParams, serverEncryptor);

        Server server = new Server(serverParams, serverStorage);
        server.start();

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
                    byte[] md5Hash = new byte[ServerStorage.MD5_HASH_LENGTH];
                    byte[] chunk = new byte[ServerStorage.CHUNK_LENGTH];

                    for (int j = 0; j < requestsNumber; j++) {
                        r.nextBytes(md5Hash);
                        r.nextBytes(chunk);

                        try {
                            client.push(md5Hash, chunk);
                            if (!Arrays.equals(chunk, client.pull(md5Hash))) {
                                log.error("Returned result is incorrect.");
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
