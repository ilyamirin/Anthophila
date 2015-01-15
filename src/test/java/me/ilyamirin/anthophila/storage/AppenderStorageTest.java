package me.ilyamirin.anthophila.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.hands.Hand;
import me.ilyamirin.anthophila.hands.InMemoryHand;
import me.ilyamirin.anthophila.indexes.InMemoryIndex;
import me.ilyamirin.anthophila.serializers.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Slf4j
public class AppenderStorageTest {

    Random r = new Random();

    @BeforeClass
    public static void before() {
        File file = new File("testHand1");
        file.delete();
        file = new File("testHand2");
        file.delete();
    }

    @Test
    public void test() throws Exception {
        Hand hand1 = new InMemoryHand();
        Hand hand2 = new InMemoryHand();

        final Storage<String, String> storage = new StorageBuilder<String, String>()
                .storageClass(AppenderStorage.class)
                .index(new InMemoryIndex())
                .hands(Sets.newHashSet(hand1, hand2))
                .keySerializer(new StringSerializer())
                .valueSerializer(new StringSerializer())
                .build();

        assertNotNull(storage);

        storage.put("key1", "value1");
        storage.put("key2", "value2");

        assertEquals("value1", storage.get("key1"));
        assertEquals("value2", storage.get("key2"));

        Callable<Exception> testProcess = new Callable<Exception>() {

            @Override
            public Exception call() throws Exception {
                for (int i = 0; i < 5000; i++)
                    try {
                        String key = UUID.randomUUID().toString();
                        String value = UUID.randomUUID().toString();

                        storage.put(key, value);

                        assertEquals(value, storage.get(key));
                    } catch (IOException e) {
                        return e;
                    }
                return null;
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(5);

        List<Future<Exception>> futures = Lists.newArrayList();

        for (int i = 0; i < 5; i++)
            futures.add(executor.submit(testProcess));

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);

        Iterable<Exception> exceptions = Iterables.transform(futures, new Function<Future<Exception>, Exception>() {
            @Override
            public Exception apply(Future<Exception> exceptionFuture) {
                try {
                    return exceptionFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }//apply
        });

        Iterables.removeIf(exceptions, Predicates.isNull());

        for (Exception e : exceptions) {
            log.error("Oops!", e);
        }

        assertFalse(exceptions.iterator().hasNext());

    }

}
