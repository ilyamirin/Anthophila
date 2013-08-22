package me.ilyamirin.anthophila.server;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author ilyamirin
 */
public interface Storage {

    public static final int CHUNK_LENGTH = 65536;
    public static final int MD5_HASH_LENGTH = 8; //long value
    public static final int AUX_CHUNK_INFO_LENGTH = 1 + MD5_HASH_LENGTH + 4; //tombstone + hash + chunk length (int)
    public static final int IV_LENGTH = 8; //IV for Salsa cipher
    public static final int ENCRYPTION_CHUNK_INFO_LENGTH = 4 + IV_LENGTH; //cipher key has in int + IV
    public static final int WHOLE_CHUNK_CELL_LENGTH = AUX_CHUNK_INFO_LENGTH + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH; //totel chunk necessarty space

    boolean contains(ByteBuffer md5Hash);

    void delete(ByteBuffer md5Hash) throws IOException;

    void append(ByteBuffer md5Hash, ByteBuffer chunk) throws IOException;

    ByteBuffer read(ByteBuffer md5Hash) throws IOException;

    void loadExistedStorage() throws IOException;
}
