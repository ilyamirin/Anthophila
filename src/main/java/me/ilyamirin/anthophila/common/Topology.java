package me.ilyamirin.anthophila.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import me.ilyamirin.anthophila.server.ServerParams;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

    @Data
    @AllArgsConstructor
    public static class Node {

        private String host;
        private int port;

        @Override
        public int hashCode() {
            return Hashing.murmur3_128().newHasher(4).putString(host).putInt(port).hash().asInt();
        }
    }

    private Map<List<Byte>, Set<Node>> keyMasks = Maps.newHashMap();
    private Map<Node, Set<List<Byte>>> nodes = Maps.newHashMap();

    public static Topology loadFromFile(String filePath) throws FileNotFoundException {
        Topology serverTopology = new Gson().fromJson(new FileReader(filePath), Topology.class);
        for (Map.Entry<List<Byte>, Set<Node>> keyMask : serverTopology.getKeyMasks().entrySet()) {
            for (Node node : keyMask.getValue()) {
                if (serverTopology.getNodes().containsKey(node)) {
                    serverTopology.getNodes().get(node).add(keyMask.getKey());
                } else {
                    serverTopology.getNodes().put(node, (Set) Sets.newHashSet(keyMask.getKey()));
                }
            }
        }
        return serverTopology;
    }

    private static boolean applyKeyToMask(ByteBuffer key, List<Byte> mask) {
        for (int i = 0; i < mask.size(); i++) {
            if (mask.get(i) == 1 && key.get(i) <= 0) {
                return false;
            } else if (mask.get(i) == 0 && key.get(i) > 0) {
                return false;
            }
        }
        return true;
    }

    public Set<Node> findNodes(ByteBuffer key) {
        for (Map.Entry<List<Byte>, Set<Node>> entry : keyMasks.entrySet()) {
            if (applyKeyToMask(key, entry.getKey()))
                return entry.getValue();
        }
        return null;
    }

    public boolean isKeyServableForServer(ByteBuffer key, ServerParams params) {
        Node node = new Node(params.getHost(), params.getPort());
        for (List<Byte> keyMask : nodes.get(node)) {
            if (applyKeyToMask(key, keyMask))
                return true;
        }
        return false;
    }

}
