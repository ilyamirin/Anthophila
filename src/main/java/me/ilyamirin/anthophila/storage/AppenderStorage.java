package me.ilyamirin.anthophila.storage;

import me.ilyamirin.anthophila.indexes.IndexEntry;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class AppenderStorage<T, E> extends Storage<T, E> {

    @Override
    IndexEntry acquireFreeSpace(int size) {
        return null;
    }
}
