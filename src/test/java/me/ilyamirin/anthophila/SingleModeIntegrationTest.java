package me.ilyamirin.anthophila;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.OneNodeClient;
import me.ilyamirin.anthophila.client.Client;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import me.ilyamirin.anthophila.common.Node;
import me.ilyamirin.anthophila.common.Pair;
import me.ilyamirin.anthophila.common.Topology;
import static org.junit.Assert.*;

/**
 * @author ilyamirin
 */
@Slf4j
public class SingleModeIntegrationTest {
                            
    private Random r = ThreadLocalRandom.current();
/*
    @Test
    public void simpleTest() throws IOException, InterruptedException {
        File file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        final String host = "127.0.0.1";
        final int port = 7619;
        
        Topology topology = new Topology();

        List<Byte> mask = Lists.newArrayList((byte) 0);
        List<Node> nodes = Lists.newArrayList(new Node(host, port));
        topology.addKeyMask(mask, nodes);

        mask = Lists.newArrayList((byte) 1);
        nodes = Lists.newArrayList(new Node(host, port));
        topology.addKeyMask(mask, nodes);

        FileWriter writer = new FileWriter("topology.json");
        new Gson().toJson(topology.getKeyMasks(), writer);
        writer.close();        

        ServerParams serverParams = new ServerParams();
        serverParams.setStorageFile("test.bin");

        serverParams.setInitialIndexSize(5000);

        serverParams.setHost(host);
        serverParams.setPort(port);

        serverParams.setEncrypt(true);
        serverParams.setNewKeysFile("new.keys");
        serverParams.setOldKeysFile("old.keys");

        serverParams.setMaxConnections(10);

        serverParams.setServeAll(true);
        
        serverParams.setTopologyFile("topology.json");

        writer = new FileWriter("server.json");
        new Gson().toJson(serverParams, ServerParams.class, writer);
        writer.close();

        Server.main();
        Thread.sleep(1000);

        final int clientsNumber = 10;
        final int requestsNumber = 1000;
        final AtomicInteger errorsCounter = new AtomicInteger(0);
        final AtomicInteger requestsPassed = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(clientsNumber);
        final List<Pair> storedChunks = Collections.synchronizedList(new ArrayList<Pair>());
                
        long start = System.currentTimeMillis();

        for (int i = 0; i < clientsNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    OneNodeClient client;
                    try {
                        client = OneNodeClient.newClient(host, port, Client.ConnectionType.OTHERS);
                    } catch (IOException ioe) {
                        errorsCounter.incrementAndGet();
                        log.error("Connection problem", ioe);
                        return;
                    }
                    
                    for (int j = 0; j < requestsNumber; j++) {
                        ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
                        ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
                        
                        r.nextBytes(key.array());
                        r.nextBytes(chunk.array());

                        try {
                            client.push(key, chunk);
                            if (!Arrays.equals(chunk.array(), client.pull(key).array())) {
                                throw new IOException("Returned result is incorrect.");
                            }
                            if (!client.seek(key)) {
                                throw new IOException("Can`t seek existed key.");
                            }
                            if (r.nextBoolean()) {
                                if (!client.remove(key)) {
                                    throw new IOException("OneNodeClient couldn`t remove chunk.");
                                }
                                if (client.pull(key) != null) {
                                    throw new IOException("OneNodeClient returned previously removed chunk.");
                                }
                                if (client.seek(key)) {
                                    throw new IOException("I have just found removed key.");
                                }
                            } else {
                                storedChunks.add(new Pair(key.array(), chunk.array()));
                            }

                        } catch (IOException ex) {
                            log.error("Oops!", ex);
                            errorsCounter.incrementAndGet();
                        }

                        if (requestsPassed.incrementAndGet() % 1000 == 0) {
                            log.info("{} pull/push/?remove/pull request quads passed.", requestsPassed.get());
                        }

                    }//for

                    try {
                        client.close();
                    } catch (IOException ex) {
                    }

                    log.info("one of clients has just finished.");
                    latch.countDown();

                }
            }.start();

        }//while

        latch.await();

        log.info("All chunks were loaded for {} seconds.", (System.currentTimeMillis() - start) / 1000);

        assertTrue(0 == errorsCounter.get());
                
        OneNodeClient client = OneNodeClient.newClient(host, port, Client.ConnectionType.OTHERS);
        int chunksChecked = 0;
        for (Pair pair : storedChunks) {
            assertTrue(client.seek(ByteBuffer.wrap(pair.getKey())));
            assertTrue(Arrays.equals(pair.getValue(), client.pull(ByteBuffer.wrap(pair.getKey())).array()));
            if (++chunksChecked % requestsNumber == 0) {
                log.info("{} loaded chunks checked.", chunksChecked);
            }
        }

        long bruteAllocatedSpace = storedChunks.size() * ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH;
        log.info("Expecting near {} Mb of disk used space.", bruteAllocatedSpace / 1000000);
        
        assertTrue(bruteAllocatedSpace * 1.1 >= file.length() - ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH);
        assertTrue(bruteAllocatedSpace * 0.9 <= file.length() - ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH);

    }//simpleTest
*/
}
