package me.ilyamirin.anthophila.server;

import com.google.common.collect.Lists;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.collections.map.MultiKeyMap;
import org.jscsi.exception.ConfigurationException;
import org.jscsi.exception.NoSuchSessionException;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;

/**
 * @author ilyamirin
 */
@Slf4j
public class ServerStorage {

    public static final int CHUNK_LENGTH = 65536;
    public static final int KEY_LENGTH = 16; //md5 hash length (16 bytes)
    public static final int AUX_CHUNK_INFO_LENGTH = 1 + KEY_LENGTH + 4; //tombstone + hash + chunk length (int)
    public static final int IV_LENGTH = 8; //IV for Salsa cipher
    public static final int ENCRYPTION_CHUNK_INFO_LENGTH = 4 + IV_LENGTH; //cipher key has in int + IV
    public static final int WHOLE_CHUNK_WITH_META_LENGTH = AUX_CHUNK_INFO_LENGTH + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH; //total chunk with meta space

    private ServerParams params;

    private ServerEnigma enigma;

    private MultiKeyMap mainIndex;
    private List<ServerIndexEntry> condemnedIndex;
    
    private Initiator initiator;
    private String target;
    private int lastBlock = 0;

    public ServerStorage(ServerParams params, ServerEnigma enigma, MultiKeyMap mainIndex, List<ServerIndexEntry> condemnedIndex, Initiator initiator) {
        this.params = params;
        this.enigma = enigma;
        this.mainIndex = mainIndex;
        this.condemnedIndex = condemnedIndex;
        this.initiator = initiator;
    }

    public static ServerStorage newServerStorage(ServerParams params, ServerEnigma serverEnigma) throws IOException, ConfigurationException, NoSuchSessionException {
        Initiator initiator = new Initiator(Configuration.create());
        initiator.createSession(params.getTarget());
        MultiKeyMap mainIndex = MultiKeyMap.decorate(new LinkedMap(params.getInitialIndexSize()));
        List<ServerIndexEntry> condemnedIndex = Lists.newArrayList();
        ServerStorage serverStorage = new ServerStorage(params, serverEnigma, mainIndex, condemnedIndex, initiator);
        return serverStorage;
    }

    public boolean contains(ByteBuffer key) {
        return mainIndex.containsKey(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12));
    }

    public synchronized void append(ByteBuffer key, ByteBuffer chunk) throws Exception {
        if (contains(key)) {
            return;
        }

        ByteBuffer bufferToWrite = ByteBuffer.allocate(AUX_CHUNK_INFO_LENGTH + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH)
                .put(Byte.MAX_VALUE) //tombstone is off
                .put(key.array()) //chunk hash
                .putInt(chunk.array().length); //chunk length

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
            int chunkPosition = lastBlock + 1;
            initiator.write(target, bufferToWrite, chunkPosition, bufferToWrite.capacity());
            ServerIndexEntry entry = new ServerIndexEntry(chunkPosition, chunk.capacity());
            mainIndex.put(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12), entry);
            lastBlock++;
            
        } else {
            ServerIndexEntry entry = condemnedIndex.get(0);
            initiator.write(target, bufferToWrite, entry.getChunkPosition(), bufferToWrite.capacity());            
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
        
        ByteBuffer bufferToRead = ByteBuffer.allocate(WHOLE_CHUNK_WITH_META_LENGTH);
        
        initiator.read(target, bufferToRead, indexEntry.getChunkPosition(), WHOLE_CHUNK_WITH_META_LENGTH);

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
            initiator.write(target, ByteBuffer.allocate(1).put(Byte.MIN_VALUE), entry.getChunkPosition(), 1);
            condemnedIndex.add(entry);
        }
    }

    public BloomFilter loadExistedStorage() throws IOException {
        log.info("Start loading data from existed database file.");

        ByteBuffer buffer = ByteBuffer.allocate(AUX_CHUNK_INFO_LENGTH);
        long chunksSuccessfullyLoaded = 0;
        long position = 0;

        BloomFilter<byte[]> filter = BloomFilter.create(Funnels.byteArrayFunnel(), params.getMaxExpectedSize(), 0.01);
   /*     
        while (fileChannel.read(buffer, position) > 0) {
            buffer.rewind();

            byte tombstone = buffer.get();

            byte[] keyArray = new byte[KEY_LENGTH];
            buffer.get(keyArray);
            ByteBuffer key = ByteBuffer.wrap(keyArray);

            int chunkLength = buffer.getInt();
            long chunkPosition = position + AUX_CHUNK_INFO_LENGTH;

            ServerIndexEntry indexEntry = new ServerIndexEntry(chunkPosition, chunkLength);

            if (tombstone == Byte.MAX_VALUE) {
                mainIndex.put(key.getInt(0), key.getInt(4), key.getInt(8), key.getInt(12), indexEntry);
                filter.put(keyArray);
            } else {
                condemnedIndex.add(indexEntry);
            }

            position = chunkPosition + ENCRYPTION_CHUNK_INFO_LENGTH + CHUNK_LENGTH;

            buffer.clear();

            if (++chunksSuccessfullyLoaded % 1000 == 0) {
                log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);
            }
        }//while
*/
        log.info("{} chunks were successfully loaded", chunksSuccessfullyLoaded);

        return filter;
    }//loadExistedStorage
}
