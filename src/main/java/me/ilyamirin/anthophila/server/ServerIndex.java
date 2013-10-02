package me.ilyamirin.anthophila.server;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ilyamirin
 */
@Slf4j
public class ServerIndex {

    private Map<Long, Map<Long, ServerIndexEntry>> index = new HashMap<>();

    public boolean contains(ByteBuffer key) {
        Long firstKey = key.getLong(0);
        if (index.containsKey(firstKey)) {
            Long secondKey = key.getLong(8);
            if (index.get(firstKey).containsKey(secondKey)) {
                return true;
            }
        }
        return false;
    }

    public void put(ByteBuffer key, ServerIndexEntry entry) {
        Long firstKey = key.getLong(0);
        Long secondKey = key.getLong(8);
        if (!index.containsKey(firstKey)) {
            Map<Long, ServerIndexEntry> innerMap = new HashMap<>();
            innerMap.put(secondKey, entry);
            index.put(firstKey, innerMap);
        } else {
            index.get(firstKey).put(secondKey, entry);
        }
    }

    public ServerIndexEntry get(ByteBuffer key) {
        Long firstKey = key.getLong(0);
        if (index.containsKey(firstKey)) {
            Long secondKey = key.getLong(8);
            return index.get(firstKey).get(secondKey);
        } else {
            return null;
        }
    }

    public ServerIndexEntry remove(ByteBuffer key) {
        Long firstKey = key.getLong(0);
        if (index.containsKey(firstKey)) {
            Long secondKey = key.getLong(8);
            return index.get(firstKey).remove(secondKey);
        } else {
            return null;
        }
    }
}
