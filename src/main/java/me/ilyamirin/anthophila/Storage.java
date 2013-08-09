package me.ilyamirin.anthophila;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface Storage {

    boolean contains(ByteBuffer md5Hash);

    void delete(ByteBuffer md5Hash) throws IOException;

    void append(ByteBuffer md5Hash, ByteBuffer chunk) throws IOException;

    ByteBuffer read(ByteBuffer md5Hash) throws IOException;

    void loadExistedStorage() throws IOException;
}
