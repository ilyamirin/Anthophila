package me.ilyamirin.anthophila.common;

import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
public interface Index {

    @Data
    @AllArgsConstructor
    public static class Entry {

        private int chunkPosition;
        private int chunkLength;
    }

    boolean containsKey(ByteBuffer key);

    boolean put(ByteBuffer key, Entry entry);
    
    Entry get(ByteBuffer key);

    Entry remove(ByteBuffer key);
}
