package me.ilyamirin.anthophila.storage;

import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.hands.Hand;
import me.ilyamirin.anthophila.indexes.IndexEntry;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Slf4j
public class AppenderStorage<T, E> extends Storage<T, E> {

    @Override
    IndexEntry getFreeSpace(final int size) throws IOException {
        Iterator<Hand> handIterator = hands.iterator();

        if (!handIterator.hasNext())
            throw new RuntimeException("This storage has no hands.");

        Hand hand = handIterator.next();
        for (; handIterator.hasNext(); ) {
            Hand candidate = handIterator.next();
            if (candidate.size() < hand.size()) {
                hand = candidate;
            }
        }

        return new IndexEntry(hand, hand.size(), size);
    }
}
