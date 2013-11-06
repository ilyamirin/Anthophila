package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface ChunkArray {

    boolean append(ByteBuffer chunk);

    boolean set(int position, ByteBuffer chunk);

    ByteBuffer get(int position, int length);

    int size();

    boolean isEmpty();
}
