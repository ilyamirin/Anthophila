package me.ilyamirin.anthophila;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.*;

/**
 * @author ilyamirin
 */
@Slf4j
public class StorageTest {

    @AllArgsConstructor
    class RandomChunksStorageClient extends Thread {

        private final Random r = new Random();
        private final ServerStorage storage;
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
                md5Hash = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
                r.nextBytes(md5Hash.array());
                chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
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
            } //while
            latch.countDown();
        }
    } //RandomChunksSender class

    private Random r = new Random();
    private ServerStorage storage;
    private File file;
    private ServerEnigma enigma;

    @Before
    public void cleanStorageFile() throws IOException {
        if (file == null) {
            file = new File("test.bin");
        }
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
    }

    public void setUp(boolean isEnctiptionOn) throws IOException {
        System.gc();

        ServerParams params = new ServerParams();
        params.setStorageFile(file.getAbsolutePath());
        params.setInitialIndexSize(5000);
        params.setEncrypt(isEnctiptionOn);

        ServerEnigma enigma = ServerEnigma.newServerEnigma(ServerEnigma.generateKeys(10), new HashMap<Integer, String>());

        storage = ServerStorage.newServerStorage(params, enigma);
        storage.loadExistedStorage();
    }

    @Test
    public void basicOpsTest() throws IOException {
        cleanStorageFile();
        setUp(false);

        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        r.nextBytes(md5Hash.array());
        ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(Arrays.equals(chunk.array(), storage.read(md5Hash).array()));

        chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH / 2);
        r.nextBytes(md5Hash.array());
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(Arrays.equals(chunk.array(), storage.read(md5Hash).array()));

        assertEquals((ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH * 2), file.length());

        storage.delete(md5Hash);

        assertFalse(storage.contains(md5Hash));
        assertNull(storage.read(md5Hash));

        assertEquals((ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH * 2), file.length());
    }

    @Test
    public void basicOpsParallelTest() throws InterruptedException, FileNotFoundException, IOException {
        cleanStorageFile();
        setUp(false);

        int concurrentClientsNumber = 10;
        int concurrentRequestsNumber = 1000;
        CountDownLatch latch = new CountDownLatch(concurrentClientsNumber);
        AtomicInteger assertionErrorsCount = new AtomicInteger(0);
        AtomicInteger passedRequests = new AtomicInteger(0);
        AtomicInteger chunksDeletedCounter = new AtomicInteger(0);
        Map<Long, ByteBuffer> existedKeys = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());
        Map<Long, ByteBuffer> existedValues = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());

        long start = System.currentTimeMillis();
        for (int i = 0; i < concurrentClientsNumber; i++) {
            new RandomChunksStorageClient(storage, latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, concurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        assertEquals(0, assertionErrorsCount.get());
        log.info("{} have been passed for {} seconds.", concurrentClientsNumber * concurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        long expectedSpace = ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH * (concurrentClientsNumber * concurrentRequestsNumber
                - chunksDeletedCounter.get());
        assertTrue((file.length() - expectedSpace) == ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH || file.length() == expectedSpace);

        //try to reload database and ask about previously addad chunks

        setUp(false);

        int counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            assertTrue(storage.contains(entry.getValue()));

            ByteBuffer result = storage.read(entry.getValue());

            assertNotNull(result);
            assertTrue(Arrays.equals(existedValues.get(entry.getKey()).array(), result.array()));

            counter++;
            if (counter % concurrentRequestsNumber == 0) {
                log.info("{} chunks were successfully checked", counter);
            }
        }//for

        log.info("{} chunks were successfully checked", counter);

        //try to execute all operations with encryption

        log.info("Turn encryption on and reload database.");
        setUp(true);

        counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            assertTrue(storage.contains(entry.getValue()));

            ByteBuffer result = storage.read(entry.getValue());

            assertNotNull(result);
            assertTrue(Arrays.equals(existedValues.get(entry.getKey()).array(), result.array()));

            counter++;
            if (counter % concurrentRequestsNumber == 0) {
                log.info("{} previously added chunks were successfully checked", counter);
            }
        }//for

        latch = new CountDownLatch(concurrentClientsNumber);
        assertionErrorsCount.set(0);
        passedRequests.set(0);
        chunksDeletedCounter.set(0);
        existedKeys.clear();
        existedValues.clear();

        start = System.currentTimeMillis();
        for (int i = 0; i < concurrentClientsNumber; i++) {
            new RandomChunksStorageClient(storage, latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, concurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        log.info("{} I/O operations have been passed for {} seconds.", concurrentClientsNumber * concurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        assertEquals(0, assertionErrorsCount.get());

        expectedSpace += ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH * (concurrentClientsNumber * concurrentRequestsNumber - chunksDeletedCounter.get());
        expectedSpace += ServerStorage.WHOLE_CHUNK_WITH_META_LENGTH * 4;

        log.info("Used space={}, max expected space= {}", file.length(), expectedSpace);
        assertTrue(file.length() <= expectedSpace);

    }//basicOpsParallelTest

}
