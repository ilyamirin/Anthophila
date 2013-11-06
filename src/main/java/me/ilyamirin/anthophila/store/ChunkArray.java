package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface ChunkArray {

    boolean set(int position, ByteBuffer chunk);

    ByteBuffer get(int position, int length);
    
    int capacity();
}
