package me.ilyamirin.anthophila;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.OneNodeClient;
import me.ilyamirin.anthophila.common.Topology;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Data;
import me.ilyamirin.anthophila.client.ClusterClient;
import me.ilyamirin.anthophila.common.Node;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author ilyamirin
 */
@Slf4j
public class IntegrationTest {

    @Data
    @AllArgsConstructor
    private class Pair {

        private ByteBuffer key;
        private ByteBuffer value;
    }

    private Random r = new Random();

    //@Ignore
    @Test
    public void simpleTest() throws IOException, InterruptedException {
        File file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        final String host = "127.0.0.1";
        final int port = 7619;

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

        FileWriter writer = new FileWriter("server.json");
        new Gson().toJson(serverParams, ServerParams.class, writer);
        writer.close();

        Server.main();
        Thread.sleep(1000);

        final int clientsNumber = 10;
        final int requestsNumber = 1000;
        final AtomicInteger errorsCounter = new AtomicInteger(0);
        final AtomicInteger requestsPassed = new AtomicInteger(0);
        final AtomicInteger chunksProcessed = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(clientsNumber);
        final List<Pair> storedChunks = Collections.synchronizedList(new ArrayList<Pair>());
        final List<ByteBuffer> removedKeys = Collections.synchronizedList(new ArrayList<ByteBuffer>());

        long start = System.currentTimeMillis();

        for (int i = 0; i < clientsNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    OneNodeClient client;
                    try {
                        client = OneNodeClient.newClient(host, port);
                    } catch (IOException ioe) {
                        errorsCounter.incrementAndGet();
                        log.error("Connection problem", ioe);
                        return;
                    }

                    ByteBuffer key = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
                    ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);

                    for (int j = 0; j < requestsNumber; j++) {
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
                                removedKeys.add(key);
                            } else {
                                chunksProcessed.incrementAndGet();
                                storedChunks.add(new Pair(key, chunk));
                            }

                        } catch (IOException ex) {
                            log.error("Oops!", ex);
                            errorsCounter.incrementAndGet();
                        }

                        //if (j % 100 == 0)
                        //    log.info("{} pull/push/?remove/pull request quads passed by {}.", j, this.hashCode());
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

        assertEquals(0, errorsCounter.get());

        OneNodeClient client = OneNodeClient.newClient(host, port);
        int chunksChecked = 0;
        for (Pair pair : storedChunks) {
            if (removedKeys.contains(pair.getKey())) {
                continue;
            }
            assertTrue(client.seek(pair.getKey()));
            assertTrue(Arrays.equals(pair.getValue().array(), client.pull(pair.getKey()).array()));
            if (++chunksChecked % 10 == 0) {
                log.info("{} chunks were checked.", requestsPassed.get());
            }

        }

        assertTrue(chunksProcessed.get() * ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH <= file.length());

    }//simpleTest

    @Ignore
    @Test
    public void clusterTest() throws IOException, InterruptedException {
        String host = "127.0.0.1";
        int port = 7620;

        Topology topology = new Topology();

        List<Byte> mask = Lists.newArrayList((byte) 0);
        Set<Node> nodes = Sets.newHashSet(new Node(host, 7620));
        topology.addKeyMask(mask, nodes);

        mask = Lists.newArrayList((byte) 1);
        nodes = Sets.newHashSet(new Node(host, 7621));
        topology.addKeyMask(mask, nodes);

        FileWriter writer = new FileWriter("topology.json");
        new Gson().toJson(topology.getKeyMasks(), writer);
        writer.close();

        int serversCount = 2;

        for (int i = 0; i < serversCount; i++) {

            String fileName = String.format("test%s.bin", i);
            File file = new File(fileName);

            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();

            ServerParams serverParams = new ServerParams();
            serverParams.setStorageFile(file.getAbsolutePath());

            serverParams.setInitialIndexSize(5000);

            serverParams.setHost(host);
            serverParams.setPort(port + i);

            serverParams.setEncrypt(true);

            serverParams.setServeAll(true);
            serverParams.setNewKeysFile("new.keys");
            serverParams.setOldKeysFile("old.keys");

            serverParams.setMaxConnections(10);

            writer = new FileWriter(String.format("server%s.json", i));
            new Gson().toJson(serverParams, ServerParams.class, writer);
            writer.close();

            Server.main(String.format("server%s.json", i));
            Thread.sleep(1000);

        }//for initial

        final ClusterClient client = ClusterClient.newClusterClient(topology);

        for (Map.Entry<Node, OneNodeClient> entry : client.getClients().entrySet()) {
            assertTrue(entry.getValue().isConnected());
        }

        final int clientsNumber = 10;
        final int requestsNumber = 1000;
        final AtomicInteger errorsCounter = new AtomicInteger(0);
        final AtomicInteger requestsPassed = new AtomicInteger(0);
        final AtomicInteger chunksStored = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(clientsNumber);
        final ConcurrentMap<ByteBuffer, ByteBuffer> chunks = Maps.newConcurrentMap();

        long start = System.currentTimeMillis();

        for (int i = 0; i < clientsNumber; i++) {
            new Thread() {
                @Override
                public void run() {
                    Random r = new Random();

                    ByteBuffer key = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
                    ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);

                    for (int j = 0; j < requestsNumber; j++) {
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
                        } catch (IOException ex) {
                            log.error("Oops!", ex);
                            errorsCounter.incrementAndGet();
                        }

                        if (r.nextBoolean()) {
                            try {
                                if (!client.remove(key)) {
                                    throw new IOException("OneNodeClient couldn`t remove chunk.");
                                }
                                if (client.pull(key) != null) {
                                    throw new IOException("OneNodeClient returned previously removed chunk.");
                                }
                                if (client.seek(key)) {
                                    throw new IOException("I found removed key.");
                                }
                            } catch (IOException exception) {
                                errorsCounter.incrementAndGet();
                                log.error("Oops!", exception);
                            }
                        } else {
                            chunksStored.incrementAndGet();
                            key.rewind();
                            chunk.rewind();
                            chunks.putIfAbsent(key, chunk);
                        }

                        //if (j % 100 == 0)
                        //    log.info("{} pull/push/?remove/pull request quads passed by {}.", j, this.hashCode());
                        if (requestsPassed.incrementAndGet() % 1000 == 0) {
                            log.info("{} pull/push/?remove/pull request quads passed.", requestsPassed.get());
                        }

                    }//for

                    log.info("one of clients has just finished.");
                    latch.countDown();

                }//run
            }.start();
        }

        latch.await();

        assertEquals(0, errorsCounter.get());
        log.info("Test was passed for {} seconds.", (System.currentTimeMillis() - start) / 1000);

        for (ConcurrentMap.Entry<ByteBuffer, ByteBuffer> entry : chunks.entrySet()) {
            entry.getKey().rewind();
            log.info("{}", entry.getKey());
            log.info("{}", client.pull(entry.getKey()));
            assertTrue(Arrays.equals(entry.getValue().array(), client.pull(entry.getKey()).array()));
        }

        client.close();

        /*

         assertTrue(chunksStored.get() * ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH <= file.length());

         */
    }//clusterTest

}
