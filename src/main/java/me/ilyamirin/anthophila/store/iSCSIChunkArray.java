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
    private final String targetName;
    private final Initiator i;
    
    private int nextBlockNumberToAdd;

    private iSCSIChunkArray(int CHUNK_SIZE, int BLOCKS_PER_CHUNK, String targetName, Initiator i, int nextBlockNumberToAdd) {
        this.CHUNK_SIZE = CHUNK_SIZE;
        this.BLOCKS_PER_CHUNK = BLOCKS_PER_CHUNK;
        this.targetName = targetName;
        this.i = i;
        this.nextBlockNumberToAdd = nextBlockNumberToAdd;
    }
    
    public static iSCSIChunkArray create(String targetName, Configuration configuration, int CHUNK_SIZE) throws Exception {
        Initiator i = new Initiator(configuration);
        i.createSession(targetName);
        int BLOCKS_PER_CHUNK = (int) (CHUNK_SIZE / i.getBlockSize(targetName));
        return new iSCSIChunkArray(CHUNK_SIZE, BLOCKS_PER_CHUNK, targetName, i, 0);
    }
    
    @Override
    public boolean append(ByteBuffer chunk) {
        try {
            i.write(targetName, chunk, nextBlockNumberToAdd, chunk.capacity());
            nextBlockNumberToAdd += BLOCKS_PER_CHUNK;
            return true;
        } catch (NoSuchSessionException | TaskExecutionException exception) {
        }
        return false;
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
    public int size() {
        return nextBlockNumberToAdd / BLOCKS_PER_CHUNK;        
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
    
}
