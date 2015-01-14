package me.ilyamirin.anthophila.storage;

import com.google.common.collect.Sets;
import me.ilyamirin.anthophila.hands.FileHand;
import me.ilyamirin.anthophila.indexes.InMemoryIndex;
import me.ilyamirin.anthophila.serializers.StringSerializer;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class StorageTest {

    @Test
    public void test() throws Exception {
        Storage<String, String> storage = new StorageBuilder<String, String>()
                .storageClass(AppenderStorage.class)
                .index(new InMemoryIndex())
                .hands(Sets.newHashSet())
                .keySerializer(new StringSerializer())
                .valueSerializer(new StringSerializer())
                .build();

        assertNotNull(storage);
    }
}
