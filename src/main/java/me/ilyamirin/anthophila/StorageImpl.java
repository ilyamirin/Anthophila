package me.ilyamirin.anthophila;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    public static final int CHUNK_LENGTH = 65536;
    public static final int AUX_CHUNK_INFO_LENGTH = 13;
    @NonNull
    private FileChannel fileChannel;
    private LongObjectOpenHashMap<IndexEntry> mainIndex = new LongObjectOpenHashMap<>();
    private LongObjectOpenHashMap<IndexEntry> condamnedIndex = new LongObjectOpenHashMap<>();

    @Override
    public boolean contains(ByteBuffer md5Hash) {
        return mainIndex.containsKey(md5Hash.getLong(0));
    }

    @Override
    public synchronized void append(ByteBuffer md5Hash, ByteBuffer chunk) throws IOException {
        long md5HashAsLong = md5Hash.getLong(0);
        int chunkLength = chunk.array().length;

        if (mainIndex.containsKey(md5HashAsLong)) {
            return;
        }

        if (condamnedIndex.containsKey(md5HashAsLong)) {
            IndexEntry entry = condamnedIndex.get(md5HashAsLong);
            long tombstonePosition = entry.getChunkPosition() - AUX_CHUNK_INFO_LENGTH;
            fileChannel.write(ByteBuffer.allocate(1).put(Byte.MAX_VALUE), tombstonePosition);
            mainIndex.put(md5HashAsLong, condamnedIndex.remove(md5HashAsLong));
            return;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(AUX_CHUNK_INFO_LENGTH + CHUNK_LENGTH)
                .put(Byte.MAX_VALUE) //tombstone is off
                .put(md5Hash) //chunk hash
                .putInt(chunk.array().length) //chunk length
                .put(chunk); //chunk itself

        byteBuffer.position(0);

        if (condamnedIndex.isEmpty()) {
            long chunkFirstBytePosition = fileChannel.size() + AUX_CHUNK_INFO_LENGTH;
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
    }

    @Override
    public ByteBuffer read(ByteBuffer md5Hash) throws IOException {
        long key = md5Hash.getLong(0);
        if (mainIndex.containsKey(key)) {
            IndexEntry indexEntry = mainIndex.get(key);
            ByteBuffer buffer = ByteBuffer.allocate(indexEntry.getChunkLength());
            fileChannel.read(buffer, indexEntry.getChunkPosition());
            return buffer;
        } else {
            return null;
        }
    }

    @Override
    public synchronized void delete(ByteBuffer md5Hash) throws IOException {
        long md5HashAsLong = md5Hash.getLong(0);
        IndexEntry indexEntry = mainIndex.get(md5HashAsLong);

        if (indexEntry != null) {
            long tombstonePosition = indexEntry.getChunkPosition() - AUX_CHUNK_INFO_LENGTH;
            fileChannel.write(ByteBuffer.allocate(1).put(Byte.MIN_VALUE), tombstonePosition);
            condamnedIndex.put(md5HashAsLong, mainIndex.remove(md5HashAsLong));
        }
    }

    @Override
    public void loadExistedStorage() throws IOException {
        log.info("Start loading data from existed database file.");

        ByteBuffer buffer = ByteBuffer.allocate(AUX_CHUNK_INFO_LENGTH);
        long chunksSuccessfullyLoaded = 0;
        long position = 0;

        while (fileChannel.read(buffer, position) > 0) {
            buffer.position(0);

            byte tombstone = buffer.get();
            long md5HashAsLong = buffer.getLong();
            int chunkLength = buffer.getInt();
            long chunkPosition = position + AUX_CHUNK_INFO_LENGTH;

            IndexEntry indexEntry = new IndexEntry(chunkPosition, chunkLength);

            if (tombstone == Byte.MAX_VALUE) {
                mainIndex.put(md5HashAsLong, indexEntry);
            } else {
                condamnedIndex.put(md5HashAsLong, indexEntry);
            }

            position = chunkPosition + CHUNK_LENGTH;
            buffer.clear();

            chunksSuccessfullyLoaded++;
            if (chunksSuccessfullyLoaded % 1000 == 0) {
                log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);
            }
        }//while

        log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);

    }//loadExistedStorage
}
