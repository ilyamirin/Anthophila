package me.ilyamirin.anthophila.hands;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by ilyamirin on 29.12.14.
 */
public interface Hand {

    long size() throws IOException;

    void write(Long position, ByteBuffer bufferToWrite) throws IOException;

    ByteBuffer read(Long position, int size) throws IOException;
}
