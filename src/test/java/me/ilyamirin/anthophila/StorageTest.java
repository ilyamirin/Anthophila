package me.ilyamirin.anthophila;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static junit.framework.Assert.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class StorageTest {

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

    public void setUp() throws IOException {
        if (aFile != null) {
            aFile.getChannel().close();
            aFile.close();
        }
        aFile = new RandomAccessFile("test.bin", "rw");
        storage = new StorageImpl(aFile.getChannel());
    }

    @Test
    public void basicOpsTest() throws IOException {
        cleanStorageFile();
        setUp();

        ByteBuffer md5Hash = ByteBuffer.allocate(8);
        r.nextBytes(md5Hash.array());
        ByteBuffer chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH);
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(chunk.equals(storage.read(md5Hash)));

        chunk = ByteBuffer.allocate(StorageImpl.CHUNK_LENGTH / 2);
        r.nextBytes(md5Hash.array());
        r.nextBytes(chunk.array());

        storage.append(md5Hash, chunk);

        assertTrue(storage.contains(md5Hash));
        assertTrue(chunk.equals(storage.read(md5Hash)));

        assertEquals((StorageImpl.CHUNK_LENGTH * 2) + (2 * 13), aFile.length());

        storage.delete(md5Hash);

        assertFalse(storage.contains(md5Hash));
        assertNull(storage.read(md5Hash));

        assertEquals((StorageImpl.CHUNK_LENGTH * 2) + (2 * 13), aFile.length());
    }

    @Test
    public void basicOpsParallelTest() throws InterruptedException, FileNotFoundException, IOException {
        cleanStorageFile();
        setUp();

        int cuncurrentClientsNumber = 10;
        final int cuncurrentRequestsNumber = 1000;
        final CountDownLatch latch = new CountDownLatch(cuncurrentClientsNumber);
        final AtomicInteger assertionErrorsCount = new AtomicInteger(0);
        final AtomicInteger passedRequests = new AtomicInteger(0);
        final AtomicInteger chunksDeletedCounter = new AtomicInteger(0);
        final Map<Long, ByteBuffer> existedKeys = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());
        final Map<Long, ByteBuffer> existedValues = Collections.synchronizedMap(new HashMap<Long, ByteBuffer>());

        for (int i = 0; i < cuncurrentClientsNumber; i++) {
            Runnable runnable = new Runnable() {
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
                            } else if (!chunk.equals(storage.read(md5Hash))) {
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
            };

            new Thread(runnable).start();

        }//for

        latch.await();

        assertEquals(0, assertionErrorsCount.get());

        long expectedSpace = (65536 + 13) * (cuncurrentClientsNumber * cuncurrentRequestsNumber
                - chunksDeletedCounter.get());
        assertTrue((aFile.length() - expectedSpace) == (65536 + 13) || aFile.length() == expectedSpace);

        setUp();

        storage.loadExistedStorage();

        int counter = 0;
        for (Map.Entry<Long, ByteBuffer> entry : existedKeys.entrySet()) {
            assertTrue(storage.contains(entry.getValue()));

            ByteBuffer result = storage.read(entry.getValue());

            assertNotNull(result);
            assertEquals(existedValues.get(entry.getKey()), result);

            counter++;
            if (counter % cuncurrentRequestsNumber == 0) {
                log.info("{} chunks were successfully checked", counter);
            }
        }//for

        log.info("{} chunks were successfully checked", counter);

    }//basicOpsParallelTest
}
