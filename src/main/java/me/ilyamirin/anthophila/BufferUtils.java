package me.ilyamirin.anthophila;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by ilyamirin on 29.12.14.
 */
public class BufferUtils {

    public static ByteBuffer emptyBuffer(int size) {
        ByteBuffer result = ByteBuffer.allocate(size);
        result.clear();
        return result;
    }

    public static ByteBuffer randomBuffer(int size) {
        byte[] array = new byte[size];
        new Random().nextBytes(array);
        return ByteBuffer.wrap(array);
    }

    public static void clearAll(ByteBuffer... buffers) {
        for (ByteBuffer buffer : buffers)
            buffer.clear();
    }

    public static boolean isEqual(ByteBuffer buffer1, ByteBuffer buffer2) {
        return Arrays.equals(buffer1.array(), buffer2.array());
    }
}
