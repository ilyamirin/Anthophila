package me.ilyamirin.anthophila;

import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.client.OneNodeClient;
import me.ilyamirin.anthophila.common.Topology;
import me.ilyamirin.anthophila.server.Server;
import me.ilyamirin.anthophila.server.ServerEnigma;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author ilyamirin
 */
@Slf4j
public class ServerTest {

    private Random r = new Random();

    @Ignore
    @Test
    public void serverMustRejectForeignKeys() throws IOException, InterruptedException {
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
        serverParams.setTopologyFile("topology.json");

        ServerEnigma serverEnigma = ServerEnigma.newServerEnigma(serverParams);
        ServerStorage serverStorage = ServerStorage.newServerStorage(serverParams, serverEnigma);
        Server server = new Server(serverParams, serverStorage, new Topology());
        server.start();

        Thread.sleep(1000);

        ByteBuffer key = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
        ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);

        r.nextBytes(key.array());
        r.nextBytes(chunk.array());

        OneNodeClient client = OneNodeClient.newClient(host, port);

        assertTrue(client.push(key, chunk));
        assertTrue(Arrays.equals(chunk.array(), client.pull(key).array()));

        client.close();
        //server.interrupt();
    }

    @Test
    public void serverMustContinueWorkingAfterBrokenOperations() throws IOException, InterruptedException {

    }

    @Test
    public void serverMustContinueWorkingAfterTimeouts() throws IOException, InterruptedException {

    }

    @Test
    public void clusteredTest() throws IOException, InterruptedException {

    }

}
