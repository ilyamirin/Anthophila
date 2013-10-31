package me.ilyamirin.anthophila.server;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.collections.map.MultiKeyMap;
import org.jscsi.exception.TaskExecutionException;
import org.jscsi.initiator.Initiator;

/**
 * @author ilyamirin
 */
@Slf4j
public class ServerStorage {

    public static final int CHUNK_LENGTH = 65536;
    public static final int KEY_LENGTH = 16;
    public static final int AUX_CHUNK_INFO_LENGTH = 1 + KEY_LENGTH + 4; //tombstone + hash + chunk length (int)
    public static final int IV_LENGTH = 8; //IV for Salsa cipher
    public static final int ENCRYPTION_CHUNK_INFO_LENGTH = 4 + IV_LENGTH; //cipher key has in int + IV

    public static final int SAN_CHUNK_CELL_SIZE = 66048;
    public static final int SAN_BLOCK_SIZE = 512;

    private ServerParams params;

    private ServerEnigma enigma;

    private MultiKeyMap mainIndex;
    private List<ServerIndexEntry> condemnedIndex;

    private Initiator initiator;
    private int lastBlockPosition = 0;

    public ServerStorage(ServerParams params, ServerEnigma enigma, MultiKeyMap mainIndex, List<ServerIndexEntry> condemnedIndex, Initiator initiator) {
        this.params = params;
        this.enigma = enigma;
        this.mainIndex = mainIndex;
        this.condemnedIndex = condemnedIndex;
        this.initiator = initiator;
    }

    public boolean contains(ByteBuffer key) {
        return mainIndex.containsKey(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12));
    }

    public synchronized void append(ByteBuffer key, ByteBuffer chunk) throws Exception {
        if (contains(key)) {
            return;
        }

        ByteBuffer bufferToWrite = ByteBuffer.allocate(SAN_CHUNK_CELL_SIZE)
                .put(Byte.MAX_VALUE) //tombstone is off      
                .put(key.array()) //chunk key
                .putInt(chunk.capacity()); //chunk length

        if (params.isEncrypt()) {
            ServerEnigma.EncryptedChunk encryptedChunk = enigma.encrypt(chunk);
            bufferToWrite
                    .putInt(encryptedChunk.getKeyHash()) //key hash
                    .put(encryptedChunk.getIV()) //IV
                    .put(encryptedChunk.getChunk().array()); //encrypted chunk
        } else {
            bufferToWrite
                    .putInt(0) //empty key hash
                    .put(new byte[IV_LENGTH]) //empty IV
                    .put(chunk.array()); //chunk itself
        }

        bufferToWrite.rewind();

        if (condemnedIndex.isEmpty()) {
            int chunkPosition = lastBlockPosition + bufferToWrite.capacity() / SAN_BLOCK_SIZE;
            initiator.write(params.getTarget(), bufferToWrite, chunkPosition, bufferToWrite.capacity());
            ServerIndexEntry entry = new ServerIndexEntry(chunkPosition, chunk.capacity());
            mainIndex.put(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12), entry);
            lastBlockPosition = chunkPosition;

        } else {
            ServerIndexEntry entry = condemnedIndex.get(0);
            initiator.write(params.getTarget(), bufferToWrite, entry.getChunkPosition(), bufferToWrite.capacity());
            condemnedIndex.remove(0);
            entry.setChunkLength(chunk.capacity());
            mainIndex.put(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12), entry);
        }

    }

    public synchronized ByteBuffer read(ByteBuffer key) throws Exception {
        ServerIndexEntry indexEntry = (ServerIndexEntry) mainIndex.get(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12));

        if (indexEntry == null) {
            return null;
        }

        ByteBuffer bufferToRead = ByteBuffer.allocate(SAN_CHUNK_CELL_SIZE);
        initiator.read(params.getTarget(), bufferToRead, indexEntry.getChunkPosition(), bufferToRead.capacity());

        Integer encryptionKeyHash = bufferToRead.getInt(AUX_CHUNK_INFO_LENGTH);

        byte[] IV = new byte[IV_LENGTH];
        bufferToRead.position(AUX_CHUNK_INFO_LENGTH + 4);
        bufferToRead.get(IV);

        ByteBuffer chunk = ByteBuffer.allocate(indexEntry.getChunkLength());
        while (chunk.hasRemaining()) {
            chunk.put(bufferToRead.get());
        }

        if (encryptionKeyHash == 0) {
            return chunk;
        } else {
            ServerEnigma.EncryptedChunk encryptedChunk = new ServerEnigma.EncryptedChunk(encryptionKeyHash, IV, chunk);
            return enigma.decrypt(encryptedChunk);
        }
    }

    public synchronized void delete(ByteBuffer key) throws Exception {
        ServerIndexEntry entry = (ServerIndexEntry) mainIndex.remove(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12));
        if (entry != null) {
            ByteBuffer tombstone = ByteBuffer.allocate(512).put(Byte.MIN_VALUE);
            tombstone.rewind();
            initiator.write(params.getTarget(), tombstone, entry.getChunkPosition(), tombstone.capacity());
            condemnedIndex.add(entry);
        }
    }

    public BloomFilter loadExistedStorage() throws Exception {
        log.info("Start loading data from existed database.");

        ByteBuffer buffer = ByteBuffer.allocate(SAN_BLOCK_SIZE);
        int chunksSuccessfullyLoaded = 0;

        BloomFilter<byte[]> filter = BloomFilter.create(Funnels.byteArrayFunnel(), params.getMaxExpectedSize(), 0.01);

        while (lastBlockPosition < initiator.getCapacity(params.getTarget())) {
            try {
                initiator.read(params.getTarget(), buffer, lastBlockPosition, buffer.capacity());
            } catch (TaskExecutionException exception) {
                log.error("Oops!", exception);
                break;
            }
                        
            buffer.rewind();
            
            byte tombstone = buffer.get();

            byte[] keyArray = new byte[KEY_LENGTH];
            buffer.get(keyArray);
            ByteBuffer key = ByteBuffer.wrap(keyArray);
            
            int chunkLength = buffer.getInt();

            ServerIndexEntry indexEntry = new ServerIndexEntry(lastBlockPosition, chunkLength);

            if (tombstone == Byte.MAX_VALUE) {
                mainIndex.put(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12), indexEntry);
                filter.put(keyArray);
            } else {
                condemnedIndex.add(indexEntry);
            }            
            
            //TODO:: it is not good
            if (condemnedIndex.size() > 5) {
                break;
            } else {
                lastBlockPosition += SAN_CHUNK_CELL_SIZE / SAN_BLOCK_SIZE;
            }
            
            buffer.clear();

            if (++chunksSuccessfullyLoaded % 1000 == 0) {
                log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);
            }
        }//while

        log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);

        return filter;
    }//loadExistedStorage
}
