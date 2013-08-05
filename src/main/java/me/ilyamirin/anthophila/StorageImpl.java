package me.ilyamirin.anthophila;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@RequiredArgsConstructor
public class StorageImpl implements Storage {

    public static final int MAX_CHUNK_LENGTH = 65536;
    @NonNull
    private FileChannel fileChannel;
    private AtomicBoolean isAccessible = new AtomicBoolean(true);
    private LongObjectOpenHashMap<Number[]> index = new LongObjectOpenHashMap<>();

    @Override
    public boolean contains(byte[] md5Hash) {
        long key = ByteBuffer.allocate(8).put(md5Hash).getLong(0);
        log.trace("{}", key);
        return index.containsKey(key);
    }

    @Override
    public synchronized void append(byte[] md5Hash, byte[] chunk) {
        try {
            //if (!isAccessible.get() && isAccessible.compareAndSet(true, false)) {
            //}

            long firstChunkBytePosition = fileChannel.size() + 13l;

            ByteBuffer byteBuffer = ByteBuffer.allocate(13 + MAX_CHUNK_LENGTH)
                    .put(Byte.MAX_VALUE) //tombstone is off
                    .put(md5Hash) //chunk hash
                    .putInt(chunk.length) //chunk length
                    .put(chunk); //chunk itself

            byteBuffer.flip();

            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer, fileChannel.size());
            }

            long key = ByteBuffer.allocate(8).put(md5Hash).getLong(0);
            index.put(key, new Number[] {firstChunkBytePosition, chunk.length});

            //while (!isAccessible.get() && isAccessible.compareAndSet(false, true)) {
            //}
        } catch (FileNotFoundException ex) {
            log.error("Oops!", ex);
        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
    }

    @Override
    public byte[] read(byte[] md5Hash) {
        try {
            long key = ByteBuffer.allocate(8).put(md5Hash).getLong(0);
            if (index.containsKey(key)) {
                Number[] chunkPositionAndLength = index.get(key);
                ByteBuffer buffer = ByteBuffer.allocate(chunkPositionAndLength[1].intValue());
                fileChannel.read(buffer, chunkPositionAndLength[0].longValue());
                return buffer.array();
            }
        } catch (FileNotFoundException ex) {
            log.error("Oops!", ex);
        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
        return null;
    }
}
