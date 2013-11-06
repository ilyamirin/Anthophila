package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import java.util.Queue;
import lombok.AllArgsConstructor;
import me.ilyamirin.anthophila.common.Index;
import me.ilyamirin.anthophila.common.Index.Entry;

/**
 *
 * @author ilyamirin
 */
@AllArgsConstructor
public class iSCSIChunkMap implements ChunkMap {

    private Index index;
    private ChunkArray chunkArray;
    private Queue<Integer> condemned;

    @Override
    public synchronized boolean containsKey(ByteBuffer key) {
        return index.containsKey(key);
    }

    @Override
    public synchronized boolean put(ByteBuffer key, ByteBuffer chunk) {
        if (index.containsKey(key)) {
            return true;
            
        } else if (condemned.isEmpty() && chunkArray.append(chunk)) {
            return index.put(key, new Index.Entry(chunkArray.size() - 1, chunk.capacity()));            
            
        } else if (chunkArray.set(condemned.peek(), chunk)) {
            return index.put(key, new Index.Entry(condemned.remove(), chunk.capacity()));            
            
        } else {
            return false;
            
        }
    }

    @Override
    public synchronized ByteBuffer get(ByteBuffer key) {
        if (!index.containsKey(key)) {
            return null;
        } else {
            Entry entry = index.get(key);
            return chunkArray.get(entry.getChunkPosition(), entry.getChunkLength());
        }
    }

    @Override
    public synchronized boolean remove(ByteBuffer key) {
        return condemned.offer(index.remove(key).getChunkPosition());
    }

}
