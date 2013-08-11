package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface Storage {

    public static final int CHUNK_LENGTH = 65536;
    public static final int MD5_HASH_LENGTH = 8;
    public static final int AUX_CHUNK_INFO_LENGTH = 13;


    boolean contains(ByteBuffer md5Hash);

    void delete(ByteBuffer md5Hash) throws IOException;

    void append(ByteBuffer md5Hash, ByteBuffer chunk) throws IOException;

    ByteBuffer read(ByteBuffer md5Hash) throws IOException;

    void loadExistedStorage() throws IOException;
}
