package me.ilyamirin.anthophila.server;

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
public class ServerStorage {

    public static final int CHUNK_LENGTH = 65536;
    public static final int MD5_HASH_LENGTH = 8; //long value
    public static final int AUX_CHUNK_INFO_LENGTH = 1 + MD5_HASH_LENGTH + 4; //tombstone + hash + chunk length (int)
    public static final int IV_LENGTH = 8; //IV for Salsa cipher
    public static final int ENCRYPTION_CHUNK_INFO_LENGTH = 4 + IV_LENGTH; //cipher key has in int + IV
    public static final int WHOLE_CHUNK_CELL_LENGTH = AUX_CHUNK_INFO_LENGTH + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH; //totel chunk necessarty space
    @NonNull
    private FileChannel fileChannel;
    @NonNull
    private ServerEncryptor enigma;
    @NonNull
    private Boolean isEncryptionOn;
    private LongObjectOpenHashMap<ServerIndexEntry> mainIndex = new LongObjectOpenHashMap<>();
    private LongObjectOpenHashMap<ServerIndexEntry> condamnedIndex = new LongObjectOpenHashMap<>();

    public boolean contains(ByteBuffer md5Hash) {
        return mainIndex.containsKey(md5Hash.getLong(0));
    }

    public synchronized void append(ByteBuffer md5Hash, ByteBuffer chunk) throws IOException {
        long md5HashAsLong = md5Hash.getLong(0);
        int chunkLength = chunk.array().length;

        if (mainIndex.containsKey(md5HashAsLong)) {
            return;
        }

        if (condamnedIndex.containsKey(md5HashAsLong)) {
            ServerIndexEntry entry = condamnedIndex.get(md5HashAsLong);
            long tombstonePosition = entry.getChunkPosition() - AUX_CHUNK_INFO_LENGTH;
            fileChannel.write(ByteBuffer.allocate(1).put(Byte.MAX_VALUE), tombstonePosition);
            mainIndex.put(md5HashAsLong, condamnedIndex.remove(md5HashAsLong));
            return;
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(AUX_CHUNK_INFO_LENGTH + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH)
                .put(Byte.MAX_VALUE) //tombstone is off
                .put(md5Hash.array()) //chunk hash
                .putInt(chunk.array().length); //chunk length

        if (isEncryptionOn) {
            ServerEncryptor.EncryptedChunk encryptedChunk = enigma.encrypt(chunk);
            byteBuffer
                    .putInt(encryptedChunk.getKeyHash()) //key hash
                    .put(encryptedChunk.getIV()) //IV
                    .put(encryptedChunk.getChunk().array()); //encrypted chunk
        } else {
            byteBuffer
                    .putInt(0) //empty key hash
                    .put(new byte[IV_LENGTH]) //empty IV
                    .put(chunk.array()); //chunk itself
        }

        byteBuffer.position(0);

        if (condamnedIndex.isEmpty()) {
            long chunkFirstBytePosition = fileChannel.size() + AUX_CHUNK_INFO_LENGTH;
            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer, fileChannel.size());
            }
            mainIndex.put(md5HashAsLong, new ServerIndexEntry(chunkFirstBytePosition, chunkLength));

        } else {
            LongObjectCursor<ServerIndexEntry> cursor = condamnedIndex.iterator().next();
            ServerIndexEntry entry = cursor.value;

            while (byteBuffer.hasRemaining()) {
                fileChannel.write(byteBuffer, entry.getChunkPosition() - 13l);
            }

            condamnedIndex.remove(cursor.key);

            entry.setChunkLength(chunkLength);
            mainIndex.put(md5HashAsLong, entry);

        }
    }

    public synchronized ByteBuffer read(ByteBuffer md5Hash) throws IOException {
        long key = md5Hash.getLong(0);
        if (mainIndex.containsKey(key)) {
            ServerIndexEntry indexEntry = mainIndex.get(key);

            ByteBuffer buffer = ByteBuffer.allocate(ENCRYPTION_CHUNK_INFO_LENGTH);
            while (buffer.hasRemaining()) {
                fileChannel.read(buffer, indexEntry.getChunkPosition());
            }

            Integer keyHash = buffer.getInt(0);
            byte[] IV = new byte[IV_LENGTH];
            buffer.position(4);
            buffer.get(IV);

            ByteBuffer chunk = ByteBuffer.allocate(indexEntry.getChunkLength());
            while (chunk.hasRemaining()) {
                fileChannel.read(chunk, indexEntry.getChunkPosition() + ENCRYPTION_CHUNK_INFO_LENGTH);
            }

            if (keyHash != 0) {
                ServerEncryptor.EncryptedChunk encryptedChunk = new ServerEncryptor.EncryptedChunk(keyHash, IV, chunk);
                return enigma.decrypt(encryptedChunk);
            } else {
                return chunk;
            }
        } else {
            return null;
        }
    }

    public synchronized void delete(ByteBuffer md5Hash) throws IOException {
        long md5HashAsLong = md5Hash.getLong(0);
        ServerIndexEntry indexEntry = mainIndex.get(md5HashAsLong);

        if (indexEntry != null) {
            long tombstonePosition = indexEntry.getChunkPosition() - AUX_CHUNK_INFO_LENGTH;
            fileChannel.write(ByteBuffer.allocate(1).put(Byte.MIN_VALUE), tombstonePosition);
            condamnedIndex.put(md5HashAsLong, mainIndex.remove(md5HashAsLong));
        }
    }

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

            ServerIndexEntry indexEntry = new ServerIndexEntry(chunkPosition, chunkLength);

            if (tombstone == Byte.MAX_VALUE) {
                mainIndex.put(md5HashAsLong, indexEntry);
            } else {
                condamnedIndex.put(md5HashAsLong, indexEntry);
            }

            position = chunkPosition + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH;
            buffer.clear();

            chunksSuccessfullyLoaded++;
            if (chunksSuccessfullyLoaded % 1000 == 0) {
                log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);
            }
        }//while

        log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);

    }//loadExistedStorage
}
