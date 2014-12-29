package me.ilyamirin.anthophila;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by ilyamirin on 29.12.14.
 */
public class BufferUtils {

    static final Random r = new Random();

    public static ByteBuffer emptyBuffer(int size) {
        ByteBuffer result = ByteBuffer.allocate(size);
        result.clear();
        return result;
    }

    public static ByteBuffer randomBuffer(int size) {
        byte[] array = new byte[size];
        r.nextBytes(array);
        return ByteBuffer.wrap(array);
    }
}
