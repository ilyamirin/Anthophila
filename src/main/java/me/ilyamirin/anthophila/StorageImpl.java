package me.ilyamirin.anthophila;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    private LongObjectOpenHashMap<IndexEntry> mainIndex = new LongObjectOpenHashMap<>();
    private LongObjectOpenHashMap<IndexEntry> condamnedIndex = new LongObjectOpenHashMap<>();

    @Override
    public boolean contains(ByteBuffer md5Hash) {
        return mainIndex.containsKey(md5Hash.getLong(0));
    }

    @Override
    public synchronized void append(ByteBuffer md5Hash, ByteBuffer chunk) {
        try {
            long md5HashAsLong = md5Hash.getLong(0);
            int chunkLength = chunk.array().length;

            if (mainIndex.containsKey(md5HashAsLong)) {
               return;
            }

            if (condamnedIndex.containsKey(md5HashAsLong)) {
                IndexEntry entry = condamnedIndex.get(md5HashAsLong);
                long tombstonePosition = entry.getChunkPosition() - 13;
                fileChannel.write(ByteBuffer.allocate(1).put(Byte.MAX_VALUE), tombstonePosition);
                mainIndex.put(md5HashAsLong, condamnedIndex.remove(md5HashAsLong));
                return;
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(13 + MAX_CHUNK_LENGTH)
                    .put(Byte.MAX_VALUE) //tombstone is off
                    .put(md5Hash) //chunk hash
                    .putInt(chunk.array().length) //chunk length
                    .put(chunk); //chunk itself

            byteBuffer.position(0);

            if (condamnedIndex.isEmpty()) {
                long chunkFirstBytePosition = fileChannel.size() + 13l;
                while (byteBuffer.hasRemaining()) {
                    fileChannel.write(byteBuffer, fileChannel.size());
                }
                mainIndex.put(md5HashAsLong, new IndexEntry(chunkFirstBytePosition, chunkLength));

            } else {
                LongObjectCursor<IndexEntry> cursor = condamnedIndex.iterator().next();
                IndexEntry entry = cursor.value;

                while (byteBuffer.hasRemaining()) {
                    fileChannel.write(byteBuffer, entry.getChunkPosition() - 13l);
                }

                condamnedIndex.remove(cursor.key);

                entry.setChunkLength(chunkLength);
                mainIndex.put(md5HashAsLong, entry);

            }

        } catch (IOException ex) {
            log.error("Oops!", ex);
        }
    }

    @Override
    public ByteBuffer read(ByteBuffer md5Hash) {
        try {
            long key = md5Hash.getLong(0);
            if (mainIndex.containsKey(key)) {
                IndexEntry indexEntry = mainIndex.get(key);
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
        long md5HashAsLong = md5Hash.getLong(0);
        IndexEntry indexEntry = mainIndex.get(md5HashAsLong);

        if (indexEntry != null) {
            try {
                long tombstonePosition = indexEntry.getChunkPosition() - 13;
                fileChannel.write(ByteBuffer.allocate(1).put(Byte.MIN_VALUE), tombstonePosition);
                condamnedIndex.put(md5HashAsLong, mainIndex.remove(md5HashAsLong));

            } catch (IOException ex) {
                log.error("Oops!", ex);
            }
        }
    }
}
