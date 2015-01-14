package me.ilyamirin.anthophila.storage;

import com.google.common.base.Preconditions;
import me.ilyamirin.anthophila.hands.Hand;
import me.ilyamirin.anthophila.indexes.Index;
import me.ilyamirin.anthophila.serializers.Serializer;
import java.util.Set;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class StorageBuilder<KEY, VALUE> {

    private Storage<KEY, VALUE> storage;

    public StorageBuilder storageClass(Class<? extends Storage> claz) throws IllegalAccessException, InstantiationException {
        storage = claz.newInstance();
        return this;
    }

    public StorageBuilder hands(Set<Hand> handSet) {
        storage.hands = handSet;
        return this;
    }

    public StorageBuilder keySerializer(Serializer<KEY> serializer) {
        storage.keySerializer = serializer;
        return this;
    }

    public StorageBuilder valueSerializer(Serializer<VALUE> serializer) {
        storage.valueSerializer = serializer;
        return this;
    }

    public StorageBuilder index(Index index) {
        storage.index = index;
        return this;
    }

    public Storage build() {
        Preconditions.checkNotNull(storage);
        Preconditions.checkNotNull(storage.index);
        Preconditions.checkNotNull(storage.keySerializer);
        Preconditions.checkNotNull(storage.valueSerializer);
        Preconditions.checkNotNull(storage.hands);
        return storage;
    }
}
