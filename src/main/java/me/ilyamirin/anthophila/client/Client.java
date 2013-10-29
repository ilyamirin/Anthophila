package me.ilyamirin.anthophila.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA. User: ilyamirin Date: 15.10.13 Time: 15:55 To
 * change this template use File | Settings | File Templates.
 */
public interface Client {

    public static class ConnectionType {

        public static final byte REPLICA = Byte.MAX_VALUE;
        public static final byte OTHERS = Byte.MIN_VALUE;
    }

    boolean push(ByteBuffer key, ByteBuffer chunk) throws IOException;

    ByteBuffer pull(ByteBuffer key) throws IOException;

    boolean remove(ByteBuffer key) throws IOException;

    boolean seek(ByteBuffer key) throws IOException;

    void close() throws IOException;
}
