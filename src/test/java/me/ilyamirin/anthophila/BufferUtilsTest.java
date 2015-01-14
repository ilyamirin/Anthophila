package me.ilyamirin.anthophila;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.*;

import static me.ilyamirin.anthophila.BufferUtils.*;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class BufferUtilsTest {

    @Test
    public void randomeBufferTest() {
        for (int i = 0; i < 1000; i++) {
            ByteBuffer buffer1 = randomBuffer(1024);
            ByteBuffer buffer2 = randomBuffer(1024);
            assertFalse(Arrays.equals(buffer1.array(), buffer2.array()));
        }
    }

    @Test
    public void clearAlltest() {
        ByteBuffer buffer1 = randomBuffer(1024);
        buffer1.position(513);

        ByteBuffer buffer2 = randomBuffer(1024);
        buffer2.position(2);

        clearAll(buffer1, buffer2);
        assertEquals(0, buffer1.position());
        assertEquals(0, buffer2.position());
    }
}
