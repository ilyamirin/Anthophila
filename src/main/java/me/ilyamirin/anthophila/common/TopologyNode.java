package me.ilyamirin.anthophila.common;

import com.google.common.hash.Hashing;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created with IntelliJ IDEA.
 * User: ilyamirin
 * Date: 09.10.13
 * Time: 12:51
 * To change this template use File | Settings | File Templates.
 */
@Data
@AllArgsConstructor
public class TopologyNode {

    private String host;
    private int port;

    @Override
    public int hashCode() {
        return Hashing.murmur3_128().newHasher(4).putString(host).putInt(port).hash().asInt();
    }
}
