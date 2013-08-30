package me.ilyamirin.anthophila;

import com.beust.jcommander.JCommander;
import com.google.common.collect.Sets;
import me.ilyamirin.anthophila.server.ServerStorage;
import me.ilyamirin.anthophila.server.ServerStorage;
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
import me.ilyamirin.anthophila.server.ServerEncryptor;
import me.ilyamirin.anthophila.server.ServerEncryptor;
import me.ilyamirin.anthophila.server.ServerIndex;
import me.ilyamirin.anthophila.server.ServerIndexEntry;
import me.ilyamirin.anthophila.server.ServerParams;
import me.ilyamirin.anthophila.server.ServerStorage;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class StorageTest {

    @AllArgsConstructor
    class RandomChunksStoragteClient extends Thread {

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
                md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
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
    private ServerEncryptor enigma = ServerEncryptor.newServerEncryptor(Sets.newHashSet(ServerEncryptor.generateKeys(10).values()), new HashSet<String>());

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
        JCommander jc = new JCommander(params, new String[]{"--encryption", "--storage", "test.bin"});
        storage = ServerStorage.newServerStorage(params, enigma);
    }

    @Test
    public void indexTest() {
        Map<ByteBuffer, ServerIndexEntry> map = new HashMap<>();

        ServerIndex serverIndex = new ServerIndex();

        for (int i = 0; i < 10000; i++) {
            byte[] bytes = new byte[16];
            r.nextBytes(bytes);
            ByteBuffer md5Hash = ByteBuffer.wrap(bytes);
            md5Hash.position(0);

            ServerIndexEntry serverIndexEntry = new ServerIndexEntry(r.nextLong(), r.nextInt());
            map.put(md5Hash, serverIndexEntry);
            serverIndex.put(md5Hash, serverIndexEntry);

            if (i % 10000 == 0) {
                log.info("{} index operations have been passed.", i);
            }
        }

        int i = 0;
        for (Map.Entry<ByteBuffer, ServerIndexEntry> entry : map.entrySet()) {
            ByteBuffer key = entry.getKey();
            assertTrue(serverIndex.contains(key));
            assertEquals(entry.getValue(), serverIndex.get(key));
            assertEquals(entry.getValue(), serverIndex.remove(key));
            assertFalse(serverIndex.contains(key));
            if (++i % 10000 == 0) {
                log.info("{} index entried were checked.", i);
            }

        }
    }

    @Test
    public void basicOpsTest() throws IOException {
        cleanStorageFile();
        setUp(false);

        ByteBuffer md5Hash = ByteBuffer.allocate(ServerStorage.MD5_HASH_LENGTH);
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

        assertEquals((ServerStorage.WHOLE_CHUNK_CELL_LENGTH * 2), file.length());

        storage.delete(md5Hash);

        assertFalse(storage.contains(md5Hash));
        assertNull(storage.read(md5Hash));

        assertEquals((ServerStorage.WHOLE_CHUNK_CELL_LENGTH * 2), file.length());
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
            new RandomChunksStoragteClient(storage, latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, cuncurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        assertEquals(0, assertionErrorsCount.get());
        log.info("{} have been passed for {} seconds.", cuncurrentClientsNumber * cuncurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        long expectedSpace = ServerStorage.WHOLE_CHUNK_CELL_LENGTH * (cuncurrentClientsNumber * cuncurrentRequestsNumber
                - chunksDeletedCounter.get());
        assertTrue((file.length() - expectedSpace) == ServerStorage.WHOLE_CHUNK_CELL_LENGTH || file.length() == expectedSpace);

        //try to reload database and ask about previously addad chunks

        setUp(false);

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
            if (counter % cuncurrentRequestsNumber == 0) {
                log.info("{} previously added chunks were successfully checked", counter);
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
            new RandomChunksStoragteClient(storage, latch, assertionErrorsCount, chunksDeletedCounter, passedRequests, existedKeys, existedValues, cuncurrentRequestsNumber)
                    .start();
        }//for

        latch.await();
        log.info("{} have been passed for {} seconds.", cuncurrentClientsNumber * cuncurrentRequestsNumber, (System.currentTimeMillis() - start) / 1000);

        assertEquals(0, assertionErrorsCount.get());

        expectedSpace += ServerStorage.WHOLE_CHUNK_CELL_LENGTH * (cuncurrentClientsNumber * cuncurrentRequestsNumber - chunksDeletedCounter.get());
        log.info("used space={}, max expected space= {}", file.length(), expectedSpace);
        //assertTrue(file.length() <= expectedSpace && file.length() >= (expectedSpace - ServerStorage.WHOLE_CHUNK_CELL_LENGTH * 4));

    }//basicOpsParallelTest
}
