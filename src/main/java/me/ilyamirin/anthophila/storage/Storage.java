package me.ilyamirin.anthophila.storage;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.hands.Hand;
import me.ilyamirin.anthophila.indexes.Index;
import me.ilyamirin.anthophila.indexes.IndexEntry;
import me.ilyamirin.anthophila.serializers.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Slf4j
public abstract class Storage<KEY, VALUE> {

    protected Set<Hand> hands;

    protected Index index;

    protected Serializer<KEY> keySerializer;
    protected Serializer<VALUE> valueSerializer;

    abstract IndexEntry getFreeSpace(int size) throws IOException;

    public boolean put(KEY key, VALUE value) throws IOException {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);

        ByteBuffer keyBuffer = keySerializer.serialize(key);

        if (index.contains(keyBuffer))
            return false;

        ByteBuffer valueBuffer = valueSerializer.serialize(value);

        IndexEntry indexEntry = getFreeSpace(valueBuffer.capacity());

        Hand workingHand = indexEntry.getHand();

        workingHand.write(indexEntry.getPosition(), valueBuffer);

        index.put(keyBuffer, indexEntry);

        return true;
    }

    public VALUE get(KEY key) throws IOException {
        Preconditions.checkNotNull(key);

        ByteBuffer keyBuffer = keySerializer.serialize(key);

        IndexEntry indexEntry = index.get(keyBuffer);

        if (indexEntry == null)
            return null;

        Hand workingHand = indexEntry.getHand();

        ByteBuffer valueBuffer = workingHand.read(indexEntry.getPosition(), indexEntry.getSize());

        return valueSerializer.deserialize(valueBuffer);
    }
}
