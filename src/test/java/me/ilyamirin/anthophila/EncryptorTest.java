package me.ilyamirin.anthophila;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.ServerEncryptor;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.bouncycastle.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class EncryptorTest {

    private Random r = new Random();
    private Gson gson = new GsonBuilder().create();
    private File file;

    @Before
    public void init() throws IOException {
        file = new File("keys.yml");
        if (file.exists()) {
            file.delete();
            file.createNewFile();
        }
    }

    @Test
    public void testEnigma() throws IOException, InterruptedException {
        int keysNumber = 10;

        Set<String> oldKeys = Sets.newHashSet();
        Map<Integer, String> newKeys = ServerEncryptor.generateKeys(keysNumber);

        assertEquals(keysNumber, newKeys.size());

        log.info("{} keys were generated", newKeys.size());
        for (String key : newKeys.values()) {
            log.info(key);
        }

        Map<String, Set<String>> keys = Maps.newHashMap();
        keys.put("oldKeys", oldKeys);
        keys.put("newKeys", Sets.newHashSet(newKeys.values()));

        FileWriter fileWriter = new FileWriter(file);

        gson.toJson(keys, fileWriter);

        fileWriter.flush();

        final ServerEncryptor enigma = null;// = ServerEncryptor.loadFromFile(file.getPath());

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
                    byte[] chunkMd5Hash = new byte[ServerStorage.MD5_HASH_LENGTH];
                    byte[] chunk = new byte[ServerStorage.CHUNK_LENGTH];
                    for (int i = 0; i < chunksPerProcess; i++) {
                        r.nextBytes(chunkMd5Hash);
                        r.nextBytes(chunk);
                        try {
                            ServerEncryptor.EncryptedChunk encryptedChunk = enigma.encrypt(ByteBuffer.wrap(chunk));
                            if (Arrays.areEqual(chunk, encryptedChunk.getChunk().array())) {
                                throw new Exception("Enigma did not encrypt chunk");
                            }
                            if (!Arrays.areEqual(chunk, enigma.decrypt(encryptedChunk).array())) {
                                throw new Exception("Enigma could not decrypt chunk");
                            }
                            usedKeys.add(encryptedChunk.getKeyHash());
                        } catch (Exception e) {
                            log.error("Oops!", e);
                            exceptionsOccured.incrementAndGet();
                        }
                        if (chunksProcessed.incrementAndGet() % 1000 == 0) {
                            log.info("{} chunks encrypted/decrypted successfully", chunksProcessed.get());
                        }
                    }//for
                    latch.countDown();
                }
            }.start();
        }//for

        latch.await();

        assertEquals(0, exceptionsOccured.get());

        assertTrue(Sets.difference(usedKeys, newKeys.keySet()).isEmpty());

        //move some keys to old and add new keys
        //old keys should not use for encryption new chunks

        //TODO::
    }//testEnigma
}
