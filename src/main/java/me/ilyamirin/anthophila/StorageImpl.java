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
    public boolean contains(ByteBuffer md5Hash) {
        return index.containsKey(md5Hash.getLong(0));
    }

    @Override
    public synchronized void append(ByteBuffer md5Hash, ByteBuffer chunk) {
        try {
            md5Hash.flip(); chunk.flip();

            long firstChunkBytePosition = fileChannel.size() + 13l;

            ByteBuffer byteBuffer = ByteBuffer.allocate(13 + MAX_CHUNK_LENGTH)
                    .put(Byte.MAX_VALUE) //tombstone is off
                    .put(md5Hash) //chunk hash
                    .putInt(chunk.array().length) //chunk length
                    .put(chunk); //chunk itself

            byteBuffer.flip();

            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer, fileChannel.size());
            }

            index.put(md5Hash.getLong(0), new Number[]{firstChunkBytePosition, chunk.array().length});

        } catch (FileNotFoundException ex) {
            log.error("Oops!", ex);
        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
    }

    @Override
    public ByteBuffer read(ByteBuffer md5Hash) {
        try {
            long key = md5Hash.getLong(0);
            if (index.containsKey(key)) {
                Number[] chunkPositionAndLength = index.get(key);
                ByteBuffer buffer = ByteBuffer.allocate(chunkPositionAndLength[1].intValue());
                fileChannel.read(buffer, chunkPositionAndLength[0].longValue());
                return buffer;
            }
        } catch (FileNotFoundException ex) {
            log.error("Oops!", ex);
        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
        return null;
    }
}
