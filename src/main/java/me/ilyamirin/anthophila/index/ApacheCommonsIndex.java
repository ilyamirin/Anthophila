package me.ilyamirin.anthophila.index;

import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.collections.map.MultiKeyMap;

/**
 *
 * @author ilyamirin
 */
public class ApacheCommonsIndex implements Index {

    private MultiKeyMap index;

    private ApacheCommonsIndex(MultiKeyMap index) {
        this.index = index;
    }
    
    public static ApacheCommonsIndex create(int initialCapacity) {
        return new ApacheCommonsIndex(MultiKeyMap.decorate(new LinkedMap(initialCapacity)));
    }
    
    @Override
    public boolean containsKey(ByteBuffer key) {
        return index.containsKey(key.get(0), key.get(4), key.get(8), key.get(12));
    }

    @Override
    public boolean put(ByteBuffer key, Entry entry) {
        index.put(key.get(0), key.get(4), key.get(8), key.get(12), entry);
        return true;
    }

    @Override
    public Entry get(ByteBuffer key) {
        return (Entry) index.get(key.get(0), key.get(4), key.get(8), key.get(12));        
    }

    @Override
    public Entry remove(ByteBuffer key) {
        return (Entry) index.remove(key.get(0), key.get(4), key.get(8), key.get(12));
    }
    
}
