package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import me.ilyamirin.anthophila.index.Index;
import me.ilyamirin.anthophila.index.Index.Entry;

/**
 *
 * @author ilyamirin
 */
public class iSCSIChunkStore implements ChunkStore {

    private Index index;
    private ChunkArray chunkArray;
    private Queue<Integer> freePositions;

    private iSCSIChunkStore(Index index, ChunkArray chunkArray, Queue<Integer> freePositions) {
        this.index = index;
        this.chunkArray = chunkArray;
        this.freePositions = freePositions;
    }

    public static iSCSIChunkStore create(Index index, ChunkArray chunkArray) {
        Queue<Integer> freePositions = new LinkedList<>();
        for (int i = 0; i < chunkArray.capacity(); i++) {
            freePositions.add(i);
        }
        return new iSCSIChunkStore(index, chunkArray, freePositions);
    }
    
    @Override
    public synchronized boolean containsKey(ByteBuffer key) {
        return index.containsKey(key);
    }

    @Override
    public synchronized boolean put(ByteBuffer key, ByteBuffer chunk) {
        if (index.containsKey(key)) {
            return true;
            
        } else if (!freePositions.isEmpty() && chunkArray.set(freePositions.peek(), chunk)) {
            return index.put(key, new Index.Entry(0, freePositions.remove(), chunk.capacity()));            
            
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
        return freePositions.offer(index.remove(key).getChunkPosition());
    }

}
