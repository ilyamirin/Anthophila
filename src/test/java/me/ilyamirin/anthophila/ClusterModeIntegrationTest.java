package me.ilyamirin.anthophila;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.ClusterClient;
import me.ilyamirin.anthophila.client.Client;
import me.ilyamirin.anthophila.client.OneNodeClient;
import me.ilyamirin.anthophila.common.Node;
import me.ilyamirin.anthophila.common.Pair;
import me.ilyamirin.anthophila.common.Topology;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class ClusterModeIntegrationTest {

    private Random r = ThreadLocalRandom.current();

    @Test
    public void clusterTest() throws IOException, InterruptedException {
        String host = "127.0.0.1";
        int port = 7620;

        Topology topology = new Topology();

        List<Byte> mask = Lists.newArrayList((byte) 0);
        List<Node> nodes = Lists.newArrayList(new Node(host, 7620), new Node(host, 7621));
        topology.addKeyMask(mask, nodes);

        mask = Lists.newArrayList((byte) 1, (byte) 0);
        nodes = Lists.newArrayList(new Node(host, 7622));
        topology.addKeyMask(mask, nodes);

        mask = Lists.newArrayList((byte) 1, (byte) 1);
        nodes = Lists.newArrayList(new Node(host, 7623));
        topology.addKeyMask(mask, nodes);

        FileWriter writer = new FileWriter("topology.json");
        new Gson().toJson(topology.getKeyMasks(), writer);
        writer.close();

        int serversCount = 4;

        for (int i = 0; i < serversCount; i++) {
            String fileName = String.format("test%s.bin", i);
            File file = new File(fileName);

            if (file.exists()) {
                file.delete();
            }
            file.createNewFile();

            ServerParams serverParams = new ServerParams();
            serverParams.setStorageFile(file.getAbsolutePath());

            serverParams.setInitialIndexSize(100);

            serverParams.setHost(host);
            serverParams.setPort(port + i);

            serverParams.setEncrypt(true);

            serverParams.setServeAll(true);
            serverParams.setNewKeysFile("new.keys");
            serverParams.setOldKeysFile("old.keys");

            serverParams.setMaxConnections(10);
            
            serverParams.setTopologyFile("topology.json");

            writer = new FileWriter(String.format("server%s.json", i));
            new Gson().toJson(serverParams, ServerParams.class, writer);
            writer.close();

            Server.main(String.format("server%s.json", i));

        }//for initial

        Thread.sleep(1500);

        ClusterClient client = ClusterClient.newClusterClient(topology);

        final int requestsNumber = 10000;
        final AtomicInteger requestsPassed = new AtomicInteger(0);
        final List<Pair> storedChunks = Collections.synchronizedList(new ArrayList<Pair>());

        long start = System.currentTimeMillis();

        for (int i = 0; i < requestsNumber; i++) {
            ByteBuffer key = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
            r.nextBytes(key.array());

            ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
            r.nextBytes(chunk.array());

            client.push(key, chunk);

            assertTrue(Arrays.equals(chunk.array(), client.pull(key).array()));
            assertTrue(client.seek(key));

            if (r.nextBoolean() && false) {
                assertTrue(client.remove(key));
                assertNull(client.pull(key));
                assertFalse(client.seek(key));
            } else {
                storedChunks.add(new Pair(key.array(), chunk.array()));
            }

            if (requestsPassed.incrementAndGet() % 1000 == 0) {
                log.info("{} pull/push/?remove/pull request quads passed.", requestsPassed.get());
            }

        }//for

        try {
            client.close();
        } catch (IOException ex) {
        }

        log.info("All chunks were loaded for {} seconds.", (System.currentTimeMillis() - start) / 1000);

        int pairsChecked = 0;
        Map<Node, OneNodeClient> clients = Maps.newHashMap();
        for (Pair pair : storedChunks) {
            ByteBuffer keyBuffer = ByteBuffer.wrap(pair.getKey());
            for (Node node : topology.findNodes(keyBuffer)) {
                if (!clients.containsKey(node)) {
                    clients.put(node, OneNodeClient.newClient(node.getHost(), node.getPort(), Client.ConnectionType.OTHERS));
                }
                OneNodeClient nodeClient = clients.get(node);
                assertTrue(nodeClient.seek(keyBuffer));
                assertTrue(Arrays.equals(pair.getValue(), nodeClient.pull(keyBuffer).array()));
            }
            if (++pairsChecked % requestsNumber == 0) {
                log.info("{} chunk pairs were checked.", pairsChecked);
            }
        }

        long expectedSpace = storedChunks.size() * ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH;
        long totalSpace = 0;

        File file = new File("test0.bin");
        assertTrue(file.length() <= expectedSpace * 1.1 / 2);
        assertTrue(file.length() >= expectedSpace * 0.9 / 2);
        totalSpace += file.length();

        file = new File("test1.bin");
        assertTrue(file.length() <= expectedSpace * 1.1 / 2);
        assertTrue(file.length() >= expectedSpace * 0.9 / 2);
        totalSpace += file.length();        

        file = new File("test2.bin");
        assertTrue(file.length() <= expectedSpace * 1.1 / 4);
        assertTrue(file.length() >= expectedSpace * 0.9 / 4);
        totalSpace += file.length();

        file = new File("test3.bin");
        assertTrue(file.length() <= expectedSpace * 1.1 / 4);
        assertTrue(file.length() >= expectedSpace * 0.9 / 4);
        totalSpace += file.length();
        
        expectedSpace += expectedSpace * 1.1 / 2; // 1 replica increases expected size

        assertTrue(totalSpace <= expectedSpace * 1.1);
        assertTrue(totalSpace >= expectedSpace * 0.9);

    }//clusterTest

}
