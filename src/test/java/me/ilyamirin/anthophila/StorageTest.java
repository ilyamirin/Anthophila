package me.ilyamirin.anthophila;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import static junit.framework.Assert.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class StorageTest {

    private Random r = new Random();
    private Storage storage;
    private File file;

    @Before
    public void setUp() throws Exception {
        file = new File("test.bin");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        RandomAccessFile aFile = new RandomAccessFile("test.bin", "rw");
        storage = new StorageImpl(aFile.getChannel());
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void basicOpsTest() {
        byte[] md5Hash = new byte[8];
        r.nextBytes(md5Hash);
        byte[] chunk = new byte[65536];
        r.nextBytes(chunk);

        storage.append(ByteBuffer.allocate(8).put(md5Hash), ByteBuffer.allocate(chunk.length).put(chunk));

        assertTrue(storage.contains(ByteBuffer.allocate(8).put(md5Hash)));
        ByteBuffer result = storage.read(ByteBuffer.allocate(8).put(md5Hash));
        assertTrue(Arrays.equals(chunk, result.array()));

        chunk = new byte[5536];
        r.nextBytes(md5Hash);
        r.nextBytes(chunk);

        storage.append(ByteBuffer.allocate(8).put(md5Hash), ByteBuffer.allocate(chunk.length).put(chunk));

        assertTrue(storage.contains(ByteBuffer.allocate(8).put(md5Hash)));
        result = storage.read(ByteBuffer.allocate(8).put(md5Hash));
        assertTrue(Arrays.equals(chunk, result.array()));


        assertEquals(65536 + 5536 + (2 * 13), file.length());
    }

    @Test
    public void basicOpsParallelTest() throws InterruptedException {
        int cuncurrentClientsNumber = 10;
        final int cuncurrentRequestsNumber = 1000;
        final CountDownLatch latch = new CountDownLatch(cuncurrentClientsNumber);
        final AtomicInteger assertioErrorsCount = new AtomicInteger(0);
        final AtomicInteger passedRequests = new AtomicInteger(0);

        for (int i = 0; i < cuncurrentClientsNumber; i++) {
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    int counter = 0;
                    byte[] md5Hash;
                    byte[] chunk;
                    while (counter < cuncurrentRequestsNumber) {
                        md5Hash = new byte[8];
                        r.nextBytes(md5Hash);
                        chunk = new byte[65536];
                        r.nextBytes(chunk);

                        storage.append(ByteBuffer.allocate(8).put(md5Hash), ByteBuffer.allocate(65536).put(chunk));

                        if (!storage.contains(ByteBuffer.allocate(8).put(md5Hash))) {
                            log.error("storage does not contains {}", md5Hash);
                            log.info("Assertion Errors Count {}", assertioErrorsCount.incrementAndGet());
                        } else {
                            ByteBuffer returnedChunk = storage.read(ByteBuffer.allocate(8).put(md5Hash));
                            if (!Arrays.equals(chunk, returnedChunk.array())) {
                                log.error("returned value does not equal to original: {} {}", chunk, returnedChunk);
                                log.info("Assertion Errors Count {}", assertioErrorsCount.incrementAndGet());
                            }
                        }

                        counter++;

                        if (passedRequests.incrementAndGet() % cuncurrentRequestsNumber == 0) {
                            log.info("{} requests passed", passedRequests.get());
                        }
                    }//while

                    log.trace("countDown client");
                    latch.countDown();
                }
            };

            new Thread(runnable).start();

        }//for

        latch.await();

        assertEquals(0, assertioErrorsCount.get());
        assertEquals((65536 + 13) * cuncurrentClientsNumber * cuncurrentRequestsNumber, file.length());
    }

}
