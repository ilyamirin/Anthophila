package me.ilyamirin.anthophila.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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

    private static final Gson GSON = new Gson();
    private static final TypeToken<Map<String, Set<Node>>> FILE_TYPE = new TypeToken<Map<String, Set<Node>>>() {};
    private static final TypeToken<List<Byte>> LIST_BYTES = new TypeToken<List<Byte>>() {};
    private Map<List<Byte>, Set<Node>> keyMasks = Maps.newHashMap();
    private Map<Node, Set<List<Byte>>> nodes = Maps.newHashMap();

    public static Topology loadFromFile(String filePath) throws FileNotFoundException {
        Map<String, Set<Node>> loadedMap = GSON.fromJson(new FileReader(filePath), FILE_TYPE.getType());
        Topology topology = new Topology();
        for (Map.Entry<String, Set<Node>> entry : loadedMap.entrySet()) {
            List<Byte> mask = GSON.fromJson(entry.getKey(), LIST_BYTES.getType());
            topology.getKeyMasks().put(mask, entry.getValue());
        }
        for (Map.Entry<List<Byte>, Set<Node>> keyMask : topology.getKeyMasks().entrySet()) {
            for (Node node : keyMask.getValue())
                if (topology.getNodes().containsKey(node))
                    topology.getNodes().get(node).add(keyMask.getKey());
                else
                    topology.getNodes().put(node, (Set) Sets.newHashSet(keyMask.getKey()));
        }
        return topology;
    }

    private static boolean applyKeyToMask(ByteBuffer key, List<Byte> mask) {
        for (int i = 0; i < mask.size(); i++)
            if (mask.get(i) == 1 && key.get(i) <= 0)
                return false;
            else if (mask.get(i) == 0 && key.get(i) > 0)
                return false;
        return true;
    }

    public Set<Node> findNodes(ByteBuffer key) {
        for (Map.Entry<List<Byte>, Set<Node>> entry : keyMasks.entrySet())
            if (applyKeyToMask(key, entry.getKey()))
                return entry.getValue();
        return null;
    }

    public boolean isKeyServableForServer(ByteBuffer key, ServerParams params) {
        Node node = new Node(params.getHost(), params.getPort());
        for (List<Byte> keyMask : nodes.get(node))
            if (applyKeyToMask(key, keyMask))
                return true;
        return false;
    }

}
