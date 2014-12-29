package me.ilyamirin.anthophila.storage;

import java.nio.ByteBuffer;

/**
 * Created by ilyamirin on 29.12.14.
 */
public interface Hand {

    boolean write(int position, ByteBuffer bufferToWrite);

    ByteBuffer read(int position, int size);
}
