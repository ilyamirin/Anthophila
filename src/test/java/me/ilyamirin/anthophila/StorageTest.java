package me.ilyamirin.anthophila;

import com.google.common.collect.Sets;
import me.ilyamirin.anthophila.server.Storage;
import me.ilyamirin.anthophila.server.StorageImpl;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static junit.framework.Assert.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.Enigma;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class StorageTest {

    @AllArgsConstructor
    private class RandomChunksStoragteClient extends Thread {

        private CountDownLatch latch;
        private AtomicInteger assertionErrorsCount;
        private AtomicInteger chunksDeletedCounter;
        private AtomicInteger passedRequests;
        private Map<Long, ByteBuffer> existedKeys;
        private Map<Long, ByteBuffer> existedValues;
        private int cuncurrentRequestsNumber;

        @Override
        public void run() {
            int counter = 0;
            ByteBuffer md5Hash;
            ByteBuffer chunk;
            while (counter < cuncurrentRequestsNumber) {
                md5Hash = ByteBuffer.allocate(8);
                r.nextBytes(md5Hash.array());
                chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH);
                r.nextBytes(chunk.array());

                try {
                    storage.append(md5Hash, chunk);

                    if (!storage.contains(md5Hash)) {
                        log.error("storage does not contains {}", md5Hash);
                        log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                    } else if (!Arrays.equals(chunk.array(), storage.read(md5Hash).array())) {
                        log.error("returned value does not equal to original.");
                        log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                    }

                    if (r.nextBoolean()) {
                        storage.delete(md5Hash);
                        if (storage.contains(md5Hash)) {
                            log.error("storage contains deleted key {}", md5Hash);
                            log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                        } else if (storage.read(md5Hash) != null) {
                            log.error("Storage returned deleted value.");
                            log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                        } else {
                            chunksDeletedCounter.incrementAndGet();
                        }
                    } else {
                        existedKeys.put(md5Hash.getLong(0), md5Hash);
                        existedValues.put(md5Hash.getLong(0), chunk);
                    }
                } catch (IOException ioe) {
                    log.error("Oops!", ioe);
                }

                counter++;

                if (passedRequests.incrementAndGet() % cuncurrentRequestsNumber == 0) {
                    log.info("{} requests passed", passedRequests.get());
                }
            }//while

            latch.countDown();
        }
    }//RandomChunksSender class
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

    public void setUp(boolean isEnctiptionOn) throws IOException {
        if (aFile != null) {
            aFile.getChannel().close();
            aFile.close();
        }
        aFile = new RandomAccessFile("test.bin", "rw");

        Enigma enigma = Enigma.newEnigma(Sets.newHashSet(Enigma.generateKeys(10).values()), new HashSet<String>());
        storage = new StorageImpl(aFile.getChannel(), enigma, isEnctiptionOn);
    }

    @Test
    public void basicOpsTest() throws IOException {
        cleanStorageFile();
        setUp(false);

        ByteBuffer md5Hash = ByteBuffer.allocate(8);
        r.nextBytes(md5Hash.array());
        ByteBuffer chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH);
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(Arrays.equals(chunk.array(), storage.read(md5Hash).array()));

        chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH / 2);
        r.nextBytes(md5Hash.array());
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(Arrays.equals(chunk.array(), storage.read(md5Hash).array()));

        assertEquals((StorageImpl.WHOLE_CHUNK_CELL_LENGTH * 2), aFile.length());

        storage.delete(md5Hash);

        assertFalse(storage.contains(md5Hash));
        assertNull(storage.read(md5Hash));

        assertEquals((StorageImpl.WHOLE_CHUNK_CELL_LENGTH * 2), aFile.length());
    }

    @Test
    public void basicOpsParallelTest() throws InterruptedException, FileNotFoundException, IOException {
        cleanStorageFile();
        setUp(false);

        int cuncurrentClientsNumber = 10;
        int cuncurrentRequestsNumber = 1000;
        CountDownLatch latch = new CountDownLatch(cuncurrentClientsNumber);
        AtomicInteger assertionErrorsCount = new AtomicInteger(0);
        AtomicInteger passedRequests = new AtomicInteger(0);
        AtomicInteger chunksDeletedCounter = new AtomicInteger(0);
        Map<Long, ByteBuffer> existedKeys = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());
        Map<Long, ByteBuffer> existedValues = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());

        long start = System.currentTimeMillis();
        for (int i = 0; i < cuncurrentClientsNumber; i++) {
            new RandomChunksStoragteClient(latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, cuncurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        log.info("{} have been passed for {} seconds.", cuncurrentClientsNumber * cuncurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        assertEquals(0, assertionErrorsCount.get());

        long expectedSpace = StorageImpl.WHOLE_CHUNK_CELL_LENGTH * (cuncurrentClientsNumber * cuncurrentRequestsNumber
                - chunksDeletedCounter.get());
        assertTrue((aFile.length() - expectedSpace) == StorageImpl.WHOLE_CHUNK_CELL_LENGTH || aFile.length() == expectedSpace);

        setUp(false);
        storage.loadExistedStorage();

        int counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            assertTrue(storage.contains(entry.getValue()));

            ByteBuffer result = storage.read(entry.getValue());

            assertNotNull(result);
            assertTrue(Arrays.equals(existedValues.get(entry.getKey()).array(), result.array()));

            counter++;
            if (counter % cuncurrentRequestsNumber == 0) {
                log.info("{} chunks were successfully checked", counter);
            }
        }//for

        log.info("{} chunks were successfully checked", counter);

        log.info("Turn encryption on and reload database.");
        setUp(true);
        storage.loadExistedStorage();

        counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            assertTrue(storage.contains(entry.getValue()));

            ByteBuffer result = storage.read(entry.getValue());

            assertNotNull(result);
            assertTrue(Arrays.equals(existedValues.get(entry.getKey()).array(), result.array()));

            counter++;
            if (counter % cuncurrentRequestsNumber == 0) {
                log.info("{} chunks were successfully checked", counter);
            }
        }//for

        latch = new CountDownLatch(cuncurrentClientsNumber);
        assertionErrorsCount.set(0);
        passedRequests.set(0);
        chunksDeletedCounter.set(0);
        existedKeys.clear();
        existedValues.clear();

        start = System.currentTimeMillis();
        for (int i = 0; i < cuncurrentClientsNumber; i++) {
            new RandomChunksStoragteClient(latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, cuncurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        log.info("{} have been passed for {} seconds.", cuncurrentClientsNumber * cuncurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        assertEquals(0, assertionErrorsCount.get());

        /*long expectedSpace = StorageImpl.WHOLE_CHUNK_CELL_LENGTH * (cuncurrentClientsNumber * cuncurrentRequestsNumber
                - chunksDeletedCounter.get());
        assertTrue((aFile.length() - expectedSpace) == StorageImpl.WHOLE_CHUNK_CELL_LENGTH || aFile.length() == expectedSpace);
        */
    }//basicOpsParallelTest

}
