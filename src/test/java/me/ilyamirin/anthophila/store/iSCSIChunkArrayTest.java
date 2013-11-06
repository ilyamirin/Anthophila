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
        
        for (int i = 0; i < maxIterations; i++) {
            byte[] chunk = new byte[CHUNK_SIZE - r.nextInt(CHUNK_SIZE / 2)];
            r.nextBytes(chunk);
            assertTrue(chunks.set(i, ByteBuffer.wrap(chunk)));
            bytes.add(chunk);
        }
        
        log.info("{} seconds were spent to process {} chunks.", (System.currentTimeMillis() - start) / 1000, maxIterations);
        
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
