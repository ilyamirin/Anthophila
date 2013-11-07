package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import me.ilyamirin.anthophila.common.Pair;
import me.ilyamirin.anthophila.index.Index;
import me.ilyamirin.anthophila.index.Index.Entry;
import org.jscsi.initiator.Configuration;

/**
 *
 * @author ilyamirin
 */
public class iSCSIChunkTarget implements ChunkTarget {

    private Index index;
    private Map<Byte, ChunkArray> chunkArrays;
    private Queue<Pair<Byte, Integer>> freePositions;

    private iSCSIChunkTarget(Index index,  Map<Byte, ChunkArray> chunkArrays, Queue<Pair<Byte, Integer>> freePositions) {
        this.index = index;
        this.chunkArrays = chunkArrays;
        this.freePositions = freePositions;
    }

    public static iSCSIChunkTarget create(Index index, Map<Byte, String> topology, Configuration configuration, int chunkSize) throws Exception {
        Map<Byte, ChunkArray> chunkArrays = new HashMap<>();
        Queue<Pair<Byte, Integer>> freePositions = new LinkedList<>();
        
        for (Map.Entry<Byte, String> entry : topology.entrySet()) {
            ChunkArray chunkArray = iSCSIChunkArray.create(entry.getValue(), configuration, chunkSize);
            chunkArrays.put(entry.getKey(), chunkArray);

            for (int i = 0; i < chunkArray.capacity(); i++) {
                freePositions.add(Pair.newPair(entry.getKey(), i));
            }            
        }
        
        return new iSCSIChunkTarget(index, chunkArrays, freePositions);
    }

    @Override
    public synchronized boolean containsKey(ByteBuffer key) {
        return index.containsKey(key);
    }

    @Override
    public synchronized boolean put(ByteBuffer key, ByteBuffer chunk) {
        if (index.containsKey(key)) {
            return true;
        } 
        
        if (freePositions.isEmpty()) {
            return false;
        }
                
        Pair<Byte, Integer> condemned = freePositions.poll();
        ChunkArray chunkArray = chunkArrays.get(condemned.getKey());         
        if (chunkArray != null) {
            chunkArray.set(condemned.getValue(), chunk);
            return index.put(key, new Index.Entry(condemned.getKey(), condemned.getValue(), chunk.capacity()));
        }
        
        return false;
    }

    @Override
    public synchronized ByteBuffer get(ByteBuffer key) {
        if (!index.containsKey(key)) {
            return null;
        } else {
            Entry entry = index.get(key);
            ChunkArray chunkArray = chunkArrays.get(entry.getArray());
            return chunkArray.get(entry.getPosition(), entry.getLength());
        }
    }

    @Override
    public synchronized boolean remove(ByteBuffer key) {
        Entry entry = index.remove(key);
        return freePositions.offer(Pair.newPair(entry.getArray(), entry.getPosition()));
    }

}
