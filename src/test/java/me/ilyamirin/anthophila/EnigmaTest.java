package me.ilyamirin.anthophila;

import com.google.common.collect.Sets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.ServerEnigma;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.bouncycastle.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

//TODO:: add old keys switch test

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class EnigmaTest {

    private Random r = new Random();

    @Test
    public void testEnigma() throws IOException, InterruptedException {
        int keysNumber = 1000;

        //Set<String> oldKeys = Sets.newHashSet();
        Map<Integer, String> newKeys = ServerEnigma.generateKeys(keysNumber);

        assertEquals(keysNumber, newKeys.size());

        EnigmaTest.log.info("{} new keys were generated", newKeys.size());
        for (String key : newKeys.values()) {
            //log.info(key);
        }

        File file = new File("new.keys");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));
        for (Map.Entry<Integer, String> entry : newKeys.entrySet()) {
            bufferedWriter.write(entry.getValue());
            bufferedWriter.newLine();
        }
        bufferedWriter.close();

        ServerParams serverParams = new ServerParams();
        serverParams.setNewKeysFile(file.getAbsolutePath());

        final ServerEnigma enigma = ServerEnigma.newServerEnigma(serverParams);

        int processesNumber = 10;
        final int chunksPerProcess = 1000;
        final AtomicInteger chunksProcessed = new AtomicInteger(0);
        final AtomicInteger exceptionsOccured = new AtomicInteger(0);
        final Set<Integer> usedKeys = Collections.synchronizedSet(new HashSet<Integer>(newKeys.size()));
        final CountDownLatch latch = new CountDownLatch(processesNumber);

        for (int j = 0; j < processesNumber; j++) {
            new Thread() {
                @Override
                public void run() {
                    byte[] chunkMd5Hash = new byte[ServerStorage.KEY_LENGTH];
                    byte[] chunk = new byte[ServerStorage.CHUNK_LENGTH];
                    for (int i = 0; i < chunksPerProcess; i++) {
                        r.nextBytes(chunkMd5Hash);
                        r.nextBytes(chunk);
                        try {
                            ServerEnigma.EncryptedChunk encryptedChunk = enigma.encrypt(ByteBuffer.wrap(chunk));
                            if (Arrays.areEqual(chunk, encryptedChunk.getChunk().array())) {
                                throw new Exception("Enigma did not encrypt chunk");
                            }
                            if (!Arrays.areEqual(chunk, enigma.decrypt(encryptedChunk).array())) {
                                throw new Exception("Enigma could not decrypt chunk");
                            }
                            usedKeys.add(encryptedChunk.getKeyHash());
                        } catch (Exception e) {
                            EnigmaTest.log.error("Oops!", e);
                            exceptionsOccured.incrementAndGet();
                        }
                        if (chunksProcessed.incrementAndGet() % 1000 == 0) {
                            EnigmaTest.log.info("{} chunks encrypted/decrypted successfully", chunksProcessed.get());
                        }
                    }//for
                    latch.countDown();
                }
            }.start();
        }//for

        latch.await();

        assertEquals(0, exceptionsOccured.get());

        //Enigma must use all keys
        Set difference = Sets.difference(newKeys.keySet(), usedKeys);
        EnigmaTest.log.info("{} keys were unused", difference.size());
        assertTrue(difference.isEmpty());

        //move some keys to old and add new keys
        //old keys should not use for encryption new chunks

        //TODO::
    }//testEnigma
}
