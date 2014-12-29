package me.ilyamirin.anthophila.storage;

import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.BufferUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Created by ilyamirin on 29.12.14.
 */
@Slf4j
public class FileHandTest {

    File file;

    @Before
    public void init() {
        file = new File("testFile");
        file.deleteOnExit();
    }

    @Test
    public void test() throws FileNotFoundException {
        ByteBuffer buffer1 = BufferUtils.randomBuffer(65536);
        ByteBuffer buffer2 = BufferUtils.randomBuffer(55555);
        ByteBuffer buffer3 = BufferUtils.randomBuffer(65536);

        FileHand writer = FileHand.create("testFile");
        assertTrue(writer.write(0, buffer1));
        assertTrue(writer.write(buffer1.capacity(), buffer2));
        assertTrue(writer.write(buffer1.capacity() + buffer2.capacity(), buffer3));

        ByteBuffer buffer1_1 = writer.read(0, 65536);
        assertEquals(buffer1_1.hashCode(), buffer1.hashCode());

        ByteBuffer buffer2_1 = writer.read(65536, 55555);
        assertEquals(buffer2_1.hashCode(), buffer2.hashCode());

        ByteBuffer buffer3_1 = writer.read(65536 + 55555, 65536);
        assertEquals(buffer3_1.hashCode(), buffer3.hashCode());
    }
}
