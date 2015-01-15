package me.ilyamirin.anthophila.indexes;

import com.google.common.collect.*;
import lombok.AllArgsConstructor;
import lombok.Data;

import static me.ilyamirin.anthophila.BufferUtils.*;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class InMemoryIndex implements Index {

    @Data
    @AllArgsConstructor
    private class InMemoryIndexEntry {

        private ByteBuffer key;
        private IndexEntry entry;
    }

    private int size = 0;

    //TODO:: add expected values setting
    private Multimap<Integer, InMemoryIndexEntry> container = HashMultimap.create();

    @Override
    public void put(ByteBuffer key, IndexEntry entry) {
        key.clear();
        synchronized (this) {
            boolean isChanged = container.put(key.hashCode(), new InMemoryIndexEntry(key, entry));
            if (isChanged) size++;
        }
    }

    @Override
    public boolean contains(ByteBuffer key) {
        key.clear();
        synchronized (container) {
            return container.containsKey(key.hashCode());
        }
    }

    @Override
    public IndexEntry get(ByteBuffer key) {
        key.clear();
        Collection<InMemoryIndexEntry> inMemoryIndexEntries;
        synchronized (this) {
            inMemoryIndexEntries = container.get(key.hashCode());
        }
        for (InMemoryIndexEntry indexEntry : inMemoryIndexEntries) {
            if (isEqual(indexEntry.getKey(), key))
                return indexEntry.getEntry();
        }
        return null;
    }

    @Override
    public IndexEntry remove(ByteBuffer key) {
        if (contains(key)) {
            key.clear();
            Collection<InMemoryIndexEntry> inMemoryIndexEntries;
            synchronized (this) {
                inMemoryIndexEntries = container.get(key.hashCode());
            }
            for (InMemoryIndexEntry entry : inMemoryIndexEntries) {
                if (isEqual(entry.getKey(), key))
                    synchronized (this) {
                        if (container.remove(key.hashCode(), entry))
                            size--;
                        else
                            throw new RuntimeException("Cant remove index entry " + entry.toString() + " by key with hash code=" + key.hashCode());
                    }
                return entry.getEntry();
            }
        }
        return null;
    }

    @Override
    public synchronized int size() {
        return size;
    }
}
