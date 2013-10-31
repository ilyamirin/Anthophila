package me.ilyamirin.anthophila;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.*;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.*;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.collections.map.MultiKeyMap;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;

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
        private final Map<Long, ByteBuffer> existedKeys;
        private Map<Long, ByteBuffer> existedValues;
        private int cuncurrentRequestsNumber;

        @Override
        public void run() {
            int counter = 0;
            while (counter < cuncurrentRequestsNumber) {
                ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
                r.nextBytes(md5Hash.array());
                ByteBuffer chunk = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
                r.nextBytes(chunk.array());
                try {
                    storage.append(md5Hash, chunk);
                    log.info("add {}", md5Hash.array());
                    if (!storage.contains(md5Hash)) {
                        log.error("storage does not contains {}", md5Hash);
                        log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                    } else if (!Arrays.equals(chunk.array(), storage.read(md5Hash).array())) {
                        log.error("returned value does not equal to original.");
                        log.info("Assertion Errors Count {}", assertionErrorsCount.incrementAndGet());
                    }
                    if (r.nextBoolean()) {
                        storage.delete(md5Hash);
                        log.info("del {}", md5Hash.array());
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
                        synchronized (existedKeys) {
                            existedKeys.put(md5Hash.getLong(0), md5Hash);
                        }
                        existedValues.put(md5Hash.getLong(0), chunk);
                    }
                } catch (Exception ioe) {
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
    private Initiator initiator;
    private ServerParams params;

    public void setUp(boolean isEnctiptionOn) throws Exception {
        System.gc();

        params = new ServerParams();
        params.setTarget("testing-target-disk1");
        params.setEncrypt(isEnctiptionOn);

        ServerEnigma enigma = ServerEnigma.newServerEnigma(ServerEnigma.generateKeys(10), new HashMap<Integer, String>());

        if (initiator != null) {
            initiator.closeSession(params.getTarget());            
        } else {
            initiator = new Initiator(Configuration.create());
        }
        initiator.createSession(params.getTarget());
        
        storage = new ServerStorage(params, enigma, MultiKeyMap.decorate(new LinkedMap(1000)), new ArrayList<ServerIndexEntry>(), initiator);
    }

    @Test
    public void basicOpsTest() throws Exception {
        setUp(false);

        ByteBuffer key1 = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        r.nextBytes(key1.array());
        ByteBuffer chunk1 = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH);
        r.nextBytes(chunk1.array());

        storage.append(key1, chunk1);

        assertTrue(storage.contains(key1));
        assertTrue(Arrays.equals(chunk1.array(), storage.read(key1).array()));

        ByteBuffer key2 = ByteBuffer.allocate(ServerStorage.KEY_LENGTH);
        r.nextBytes(key2.array());
        ByteBuffer chunk2 = ByteBuffer.allocate(ServerStorage.CHUNK_LENGTH / 2);
        r.nextBytes(chunk2.array());
        
        storage.append(key2, chunk2);

        assertTrue(storage.contains(key2));
        assertTrue(Arrays.equals(chunk2.array(), storage.read(key2).array()));

        storage.delete(key1);

        assertFalse(storage.contains(key1));
        assertNull(storage.read(key1));        
    }

    @Test
    public void basicOpsParallelTest() throws Exception {
        setUp(false);

        int concurrentClientsNumber = 10;
        int concurrentRequestsNumber = 10;
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

        //try to reload database and ask about previously addad chunks

        setUp(false);

        storage.loadExistedStorage();
        
        int counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            log.info("check {}", entry.getValue().array());
            assertTrue(storage.contains(entry.getValue()));
            assertTrue(Arrays.equals(entry.getValue().array(), storage.read(entry.getValue()).array()));

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

    }//basicOpsParallelTest

}
