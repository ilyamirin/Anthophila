package me.ilyamirin.anthophila;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
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
    private LongObjectOpenHashMap<IndexEntry> index = new LongObjectOpenHashMap<>();

    @Override
    public boolean contains(ByteBuffer md5Hash) {
        return index.containsKey(md5Hash.getLong(0));
    }

    @Override
    public synchronized void append(ByteBuffer md5Hash, ByteBuffer chunk) {
        try {
            long md5HashAsLong = md5Hash.getLong(0);
            long firstChunkBytePosition = fileChannel.size() + 13l;
            int chunkLength = chunk.array().length;

            ByteBuffer byteBuffer = ByteBuffer.allocate(13 + MAX_CHUNK_LENGTH)
                    .put(Byte.MAX_VALUE) //tombstone is off
                    .put(md5Hash) //chunk hash
                    .putInt(chunk.array().length) //chunk length
                    .put(chunk); //chunk itself

            byteBuffer.position(0);

            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer, fileChannel.size());
            }

            index.put(md5HashAsLong, new IndexEntry(firstChunkBytePosition, chunkLength));

        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
    }

    @Override
    public ByteBuffer read(ByteBuffer md5Hash) {
        try {
            long key = md5Hash.getLong(0);
            if (index.containsKey(key)) {
                IndexEntry indexEntry = index.get(key);
                ByteBuffer buffer = ByteBuffer.allocate(indexEntry.getChunkLength());
                fileChannel.read(buffer, indexEntry.getChunkPosition());
                return buffer;
            }
        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
        return null;
    }

    @Override
    public synchronized void delete(ByteBuffer md5Hash) {
        IndexEntry indexEntry = index.get(md5Hash.getLong(0));
        if (indexEntry != null) {
            try {
                long tombstonePosition = indexEntry.getChunkPosition() - 13;
                fileChannel.write(ByteBuffer.allocate(1).put(Byte.MIN_VALUE), tombstonePosition);
                index.remove(md5Hash.getLong(0));
            } catch (IOException ex) {
                log.error("Oops!", ex);
            }
        }
    }
}
