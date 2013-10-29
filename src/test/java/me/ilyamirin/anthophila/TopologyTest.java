package me.ilyamirin.anthophila;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import me.ilyamirin.anthophila.common.Topology;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import me.ilyamirin.anthophila.common.Node;

import static org.junit.Assert.*;

/**
 *
 * @author ilyamirin
 */
@Slf4j
public class TopologyTest {

    private static final Gson GSON = new Gson();

    private void testTopology(Topology topology) {
        List<Byte> first = Lists.newArrayList((byte) 0);
        List<Node> nodesForFirst = Lists.newArrayList(new Node("127.0.0.1", 999), new Node("127.0.0.4", 999));
        topology.getKeyMasks().put(first, nodesForFirst);

        List<Byte> second = Lists.newArrayList((byte) 1, (byte) 0);
        List<Node> nodesForSecond = Lists.newArrayList(new Node("127.0.0.2", 999));
        topology.getKeyMasks().put(second, nodesForSecond);

        List<Byte> third = Lists.newArrayList((byte) 1, (byte) 1);
        List<Node> nodesForThird = Lists.newArrayList(new Node("127.0.0.3", 999));
        topology.getKeyMasks().put(third, nodesForThird);

        byte[] keyForFirstNode = new byte[]{0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
        assertEquals(topology.findNodes(ByteBuffer.wrap(keyForFirstNode)), nodesForFirst);

        byte[] keyForSecondNode = new byte[]{1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
        assertEquals(topology.findNodes(ByteBuffer.wrap(keyForSecondNode)), nodesForSecond);

        byte[] keyForThirdNode = new byte[]{1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0};
        assertEquals(topology.findNodes(ByteBuffer.wrap(keyForThirdNode)), nodesForThird);
    }

    @Test
    public void testTopology() throws IOException {
        Topology topology = new Topology();
        testTopology(topology);

        FileWriter fw = new FileWriter("topology.json");
        GSON.toJson(topology.getKeyMasks(), fw);
        fw.close();

        Topology loadedTopology = Topology.loadFromFile("topology.json");
        assertTrue(Sets.symmetricDifference(loadedTopology.getKeyMasks().keySet(), topology.getKeyMasks().keySet()).isEmpty());
        testTopology(loadedTopology);
    }//testEnigma

    @Test
    public void testNodeClass() {
        assertEquals(new Node("127.0.0.1", 999), new Node("127.0.0.1", 999));
        assertNotEquals(new Node("127.0.0.1", 999), new Node("127.0.0.4", 999));
        assertNotEquals(new Node("127.0.0.1", 999), new Node("127.0.0.1", 1001));
    }
}
