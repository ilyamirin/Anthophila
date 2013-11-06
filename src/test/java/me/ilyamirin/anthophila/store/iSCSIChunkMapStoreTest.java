package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.index.ApacheCommonsIndex;
import me.ilyamirin.anthophila.index.Index;
import org.jscsi.initiator.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class iSCSIChunkMapStoreTest {
    
    private Random r = new Random();
    
    @Test
    public void test() throws Exception {
        int entriesCounter = 1000;
        
        Index index = ApacheCommonsIndex.create(entriesCounter);        
        ChunkArray chunkArray = iSCSIChunkArray.create("testing-target-disk1", Configuration.create(), 65536);
        ChunkStore chunkMap = iSCSIChunkStore.create(index, chunkArray);
        
        long start = System.currentTimeMillis();
        
        for (int i = 0; i < entriesCounter; i++) {
            byte[] key = new byte[16];
            r.nextBytes(key);
            byte[] chunk = new byte[65536 - r.nextInt(65536 / 2)];
            r.nextBytes(chunk);
            
            assertTrue(chunkMap.put(ByteBuffer.wrap(key), ByteBuffer.wrap(chunk)));            
            assertTrue(Arrays.equals(chunkMap.get(ByteBuffer.wrap(key)).array(), chunk)); 
        }
        
        log.info("{} chunks were putted and getted for {} seconds", entriesCounter, (System.currentTimeMillis() - start) / 1000);
    }
}
