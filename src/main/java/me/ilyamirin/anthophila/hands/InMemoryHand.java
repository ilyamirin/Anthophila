package me.ilyamirin.anthophila.hands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by ilyamirin on 15.01.15.
 */
public class InMemoryHand implements Hand {

    private long size = 0;
    private Map<Long, ByteBuffer> container = Collections.synchronizedMap(new TreeMap<Long, ByteBuffer>());

    @Override
    public synchronized long size() throws IOException {
        return size;
    }

    @Override
    public synchronized void write(Long position, ByteBuffer bufferToWrite) throws IOException {
        container.put(position, bufferToWrite);
        size += bufferToWrite.capacity();
    }

    @Override
    public synchronized ByteBuffer read(Long position, int size) throws IOException {
        return container.get(position);
    }
}
