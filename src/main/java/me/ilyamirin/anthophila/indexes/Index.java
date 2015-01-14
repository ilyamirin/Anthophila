package me.ilyamirin.anthophila.indexes;

import java.nio.ByteBuffer;

/**
 * Created by ilyamirin on 14.01.15.
 */
public interface Index {

    void put(ByteBuffer key, IndexEntry entry);

    boolean contains(ByteBuffer key);

    /**
     *
     * @param key
     * @return IndexEntry of this key if it is existed - null otherwise.
     */
    IndexEntry get(ByteBuffer key);

    /**
     *
     * @param key
     * @return IndexEntry of this key if it is existed - null otherwise.
     */
    IndexEntry remove(ByteBuffer key);

    int size();
}
