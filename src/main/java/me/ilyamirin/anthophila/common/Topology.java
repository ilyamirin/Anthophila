package me.ilyamirin.anthophila.common;

import lombok.Data;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ilyamirin
 * Date: 09.10.13
 * Time: 12:15
 * To change this template use File | Settings | File Templates.
 */
@Data
public class Topology {

    private Set<TopologyKeyMask> keyMasks = new HashSet<>();

    public Set<TopologyNode> findNodes(ByteBuffer key) {
        for (TopologyKeyMask keyMask : keyMasks) {
            if (keyMask.applyKeyToMask(key)) {
                return keyMask.getNodes();
            }
        }
        return null;
    }
}
