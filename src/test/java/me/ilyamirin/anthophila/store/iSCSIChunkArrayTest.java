package me.ilyamirin.anthophila.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.jscsi.initiator.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class iSCSIChunkArrayTest {
    
    private static final int CHUNK_SIZE = 65536;
    private Random r = new Random();
    
    @Test
    public void test() throws Exception {
        Configuration configuration = Configuration.create();
        iSCSIChunkArray chunks = iSCSIChunkArray.create("testing-target-disk1", configuration, CHUNK_SIZE);
        List<byte[]> bytes = new ArrayList<>();        
        int maxIterations = 1000;
        
        long start = System.currentTimeMillis();
        long totalSize = 0;
        
        Thread.sleep(10);
        
        for (int i = 0; i < maxIterations; i++) {
            byte[] chunk = new byte[CHUNK_SIZE - r.nextInt(CHUNK_SIZE / 2)];
            totalSize += chunk.length;
            r.nextBytes(chunk);
            assertTrue(chunks.set(i, ByteBuffer.wrap(chunk)));
            bytes.add(chunk);
            if (i != 0 && (i % 100 == 0)) {
                log.warn("{} seconds were spent to process {} chunks ({} MB/s).", (System.currentTimeMillis() - start) / 1000, i, i * CHUNK_SIZE / 1024 / 1024 / ((System.currentTimeMillis() - start) / 100));
            }
        }          
        
        long spentTime = (System.currentTimeMillis() - start) / 1000;
        
        log.warn("{} seconds were spent to send {} chunks of {} MB.", spentTime, maxIterations, totalSize / 1000000);        
        
        for (int i = 0; i < maxIterations; i++) {
            assertTrue(Arrays.equals(bytes.get(i), chunks.get(i, bytes.get(i).length).array()));
        }        
        
        int changeChunksCounter = maxIterations / 2;
        while (changeChunksCounter > 0) {
            int position = r.nextInt(bytes.size());
            byte[] chunk = new byte[CHUNK_SIZE - r.nextInt(CHUNK_SIZE / 2)];
            r.nextBytes(chunk);
            assertTrue(chunks.set(position, ByteBuffer.wrap(chunk)));
            bytes.set(position, chunk);
            changeChunksCounter--;
        }

        for (int i = 0; i < maxIterations; i++) {
            assertTrue(Arrays.equals(bytes.get(i), chunks.get(i, bytes.get(i).length).array()));
        }       
                
    }
}
