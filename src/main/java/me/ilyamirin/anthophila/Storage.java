package me.ilyamirin.anthophila;

import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface Storage {

    boolean contains(ByteBuffer md5Hash);

    void delete(ByteBuffer md5Hash);

    void append(ByteBuffer md5Hash, ByteBuffer chunk);

    ByteBuffer read(ByteBuffer md5Hash);
}
