package me.ilyamirin.anthophila.common;

import com.google.common.hash.Hashing;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ilyamirin
 * Date: 09.10.13
 * Time: 12:52
 * To change this template use File | Settings | File Templates.
 */
@Data
public class TopologyKeyMask {

    private byte[] mask;
    private Set<TopologyNode> nodes = new HashSet<>();

    public boolean applyKeyToMask(ByteBuffer key) {
        for (int i = 0; i < mask.length; i++) {
            if (mask[i] == 1 && key.get(i) <= 0) {
                return false;
            } else if (mask[i] == 0 && key.get(i) > 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean applyKeyToMask(ByteBuffer key, byte[] mask) {
        for (int i = 0; i < mask.length; i++) {
            if (mask[i] == 1 && key.get(i) <= 0) {
                return false;
            } else if (mask[i] == 0 && key.get(i) > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Hashing.murmur3_128().hashBytes(mask).asInt();
    }
}
