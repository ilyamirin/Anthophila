package me.ilyamirin.anthophila.hands;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by ilyamirin on 29.12.14.
 */
public interface Hand {

    long size() throws IOException;

    void write(int position, ByteBuffer bufferToWrite) throws IOException;

    ByteBuffer read(int position, int size) throws IOException;
}
