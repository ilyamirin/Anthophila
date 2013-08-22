package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author ilyamirin
 */
@Slf4j
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
            md5Hash = ByteBuffer.allocate(8);
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
