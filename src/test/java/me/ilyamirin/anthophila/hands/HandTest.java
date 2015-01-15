package me.ilyamirin.anthophila.hands;

import com.google.common.base.Preconditions;
import me.ilyamirin.anthophila.BufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Created by ilyamirin on 15.01.15.
 */
public abstract class HandTest {

    Hand writer;

    void innerTest() throws IOException{
        Preconditions.checkNotNull(writer);

        ByteBuffer buffer1 = BufferUtils.randomBuffer(65536);
        ByteBuffer buffer2 = BufferUtils.randomBuffer(55555);
        ByteBuffer buffer3 = BufferUtils.randomBuffer(65536);

        assertEquals(0, writer.size());

        writer.write(0l, buffer1);
        assertEquals(buffer1.capacity(), writer.size());

        writer.write(new Long(buffer1.capacity()), buffer2);
        assertEquals(buffer1.capacity() + buffer2.capacity(), writer.size());

        writer.write(new Long(buffer1.capacity() + buffer2.capacity()), buffer3);
        assertEquals(buffer1.capacity() + buffer2.capacity() + buffer3.capacity(), writer.size());

        ByteBuffer buffer1_1 = writer.read(0l, 65536);
        assertEquals(buffer1_1.hashCode(), buffer1.hashCode());

        ByteBuffer buffer2_1 = writer.read(65536l, 55555);
        assertEquals(buffer2_1.hashCode(), buffer2.hashCode());

        ByteBuffer buffer3_1 = writer.read(65536l + 55555, 65536);
        assertEquals(buffer3_1.hashCode(), buffer3.hashCode());
    }

    public abstract void test() throws IOException;
}
