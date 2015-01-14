package me.ilyamirin.anthophila.indexes;

import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.hands.FileHand;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static me.ilyamirin.anthophila.BufferUtils.randomBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Slf4j
public class InMemoryIndexTest {

    private Random r = new Random();

    @Test
    public void test() throws InterruptedException {
        final Index index = new InMemoryIndex();

        ByteBuffer key = randomBuffer(16);
        IndexEntry indexEntry = new IndexEntry(null, 0, 1024);

        index.put(key, indexEntry);

        assertTrue(index.contains(key));

        assertEquals(indexEntry, index.get(key));

        assertEquals(1, index.size());

        assertEquals(indexEntry, index.remove(key));

        assertEquals(0, index.size());

        //multytest

        final AtomicInteger expectedIndexSize = new AtomicInteger(0);

        Runnable thread = new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    ByteBuffer key = randomBuffer(16);
                    IndexEntry indexEntry = new IndexEntry(null, r.nextInt(), 1024);
                    index.put(key, indexEntry);
                    assertTrue(index.contains(key));
                    assertEquals(indexEntry, index.get(key));
                    index.size();
                    if (r.nextBoolean())
                        assertEquals(indexEntry, index.remove(key));
                    else
                        expectedIndexSize.incrementAndGet();
                }
            }
        };//runnable

        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++)
            executor.execute(thread);
        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.SECONDS);

        assertEquals(expectedIndexSize.get(), index.size());
    }
}
