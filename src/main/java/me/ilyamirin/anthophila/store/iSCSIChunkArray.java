package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import org.jscsi.exception.NoSuchSessionException;
import org.jscsi.exception.TaskExecutionException;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;

/**
 *
 * @author ilyamirin
 */
public class iSCSIChunkArray implements ChunkArray {

    private final int CHUNK_SIZE;
    private final int BLOCKS_PER_CHUNK;
    private final int capacity;
    private final String targetName;    
    private final Initiator i;
    
    private iSCSIChunkArray(int CHUNK_SIZE, int BLOCKS_PER_CHUNK, String targetName, Initiator i, int capacity) {
        this.CHUNK_SIZE = CHUNK_SIZE;
        this.BLOCKS_PER_CHUNK = BLOCKS_PER_CHUNK;
        this.targetName = targetName;
        this.i = i;
        this.capacity = capacity;
    }
    
    public static iSCSIChunkArray create(String targetName, Configuration configuration, int CHUNK_SIZE) throws Exception {
        Initiator i = new Initiator(configuration);
        i.createSession(targetName);
        int BLOCKS_PER_CHUNK = (int) (CHUNK_SIZE / i.getBlockSize(targetName));
        int capasity = (int) (i.getCapacity(targetName) / BLOCKS_PER_CHUNK);
        return new iSCSIChunkArray(CHUNK_SIZE, BLOCKS_PER_CHUNK, targetName, i, capasity);
    }
        
    @Override
    public boolean set(int index, ByteBuffer chunk) {
        if (chunk.capacity() <= CHUNK_SIZE) {
            try {
                i.write(targetName, chunk, index * BLOCKS_PER_CHUNK, chunk.capacity());
                return true;
            } catch (NoSuchSessionException ex) {
            } catch (TaskExecutionException ex) {
            }
        }
        return false;
    }

    @Override
    public ByteBuffer get(int position, int length) {
        ByteBuffer chunk = ByteBuffer.allocate(length);
        if (chunk.capacity() <= CHUNK_SIZE) {
            try {
                i.read(targetName, chunk, position * BLOCKS_PER_CHUNK, length);
                return chunk;
            } catch (NoSuchSessionException ex) {
            } catch (TaskExecutionException ex) {
            }
        }
        return chunk;
    }

    @Override
    public int capacity() {
        return capacity;
    }
        
}
