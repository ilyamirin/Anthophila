package me.ilyamirin.anthophila.server;

import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.StreamCipher;
import org.bouncycastle.crypto.engines.Salsa20Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static java.util.Collections.synchronizedMap;

/**
 * @author ilyamirin
 */
@Slf4j
public class ServerEnigma {

    @Data
    @AllArgsConstructor
    public static class EncryptedChunk {

        private Integer keyHash;
        private byte[] IV;
        private ByteBuffer chunk;
    }

    private Random r = new Random();
    private Map<Integer, String> oldKeys;
    private Map<Integer, String> keys;
    private List<Integer> newKeysHashes;

    private ServerEnigma(Map<Integer, String> oldKeys, Map<Integer, String> keys, List<Integer> newKeysHashes) {
        this.oldKeys = oldKeys;
        this.keys = keys;
        this.newKeysHashes = newKeysHashes;
    }

    public static ServerEnigma newServerEnigma(Map<Integer, String> keys, Map<Integer, String> oldKeys) throws IOException {
        List<Integer> newKeysHashes = Collections.synchronizedList(new ArrayList<Integer>());
        for (Integer key : keys.keySet())
            newKeysHashes.add(key);
        return new ServerEnigma(synchronizedMap(oldKeys), synchronizedMap(keys), newKeysHashes);
    }

    public static ServerEnigma newServerEnigma(ServerParams serverParams) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(serverParams.getActualKeysFile()));
        Map<Integer, String> keys = synchronizedMap(new HashMap<Integer, String>());
        List<Integer> newKeysHashes = Collections.synchronizedList(new ArrayList<Integer>());
        while (bufferedReader.ready()) {
            String newKey = bufferedReader.readLine();
            keys.put(newKey.hashCode(), newKey);
            newKeysHashes.add(newKey.hashCode());
        }
        bufferedReader.close();

        log.info("{} new keys were loaded for encryption.", keys.size());

        Map<Integer, String> oldKeys = synchronizedMap(new HashMap<Integer, String>());
        if (serverParams.getOldKeysFile() != null) {
            bufferedReader = new BufferedReader(new FileReader(serverParams.getOldKeysFile()));
            while (bufferedReader.ready()) {
                String oldKey = bufferedReader.readLine();
                oldKeys.put(oldKey.hashCode(), oldKey);
            }
            bufferedReader.close();
        }

        return new ServerEnigma(oldKeys, keys, newKeysHashes);
    }

    public static Map<Integer, String> generateKeys(int number) {
        final String SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
        Random r = new Random();
        Map<Integer, String> keys = Maps.newHashMap();
        String key;
        for (int i = 0; i < number; i++) {
            key = "";
            for (int j = 0; j < 256; j++) {
                int symbolPos = r.nextInt(SYMBOLS.length());
                key += SYMBOLS.substring(symbolPos, symbolPos + 1);
            }
            keys.put(key.hashCode(), key);
        }//for
        return keys;
    }

    public EncryptedChunk encrypt(ByteBuffer chunk) {
        //select random key
        Integer keyHash = newKeysHashes.get(r.nextInt(newKeysHashes.size()));
        String key = keys.get(keyHash);

        //generate random IV
        byte[] IV = new byte[ServerStorage.IV_LENGTH];
        r.nextBytes(IV);

        CipherParameters cipherParameters = new ParametersWithIV(new KeyParameter(key.getBytes()), IV);

        StreamCipher cipher = new Salsa20Engine();
        cipher.init(true, cipherParameters);

        byte[] result = new byte[chunk.capacity()];
        cipher.processBytes(chunk.array(), 0, chunk.capacity(), result, 0);

        return new EncryptedChunk(keyHash, IV, ByteBuffer.wrap(result));
    }

    public ByteBuffer decrypt(EncryptedChunk encryptedChunk) {
        Integer keyHash = encryptedChunk.getKeyHash();
        String key = keys.containsKey(keyHash) ? keys.get(keyHash) : oldKeys.get(keyHash);

        CipherParameters cipherParameters = new ParametersWithIV(new KeyParameter(key.getBytes()), encryptedChunk.getIV());

        StreamCipher cipher = new Salsa20Engine();
        cipher.init(true, cipherParameters);

        byte[] result = new byte[encryptedChunk.getChunk().capacity()];
        cipher.processBytes(encryptedChunk.getChunk().array(), 0, encryptedChunk.getChunk().capacity(), result, 0);

        return ByteBuffer.wrap(result);
    }
}
