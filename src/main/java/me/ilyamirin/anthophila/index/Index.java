package me.ilyamirin.anthophila.index;

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

        private byte array;
        private int position;
        private int length;
    }

    boolean containsKey(ByteBuffer key);

    boolean put(ByteBuffer key, Entry entry);
    
    Entry get(ByteBuffer key);

    Entry remove(ByteBuffer key);
}
