package me.ilyamirin.anthophila;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.TopologyNode;
import org.junit.Test;

import me.ilyamirin.anthophila.common.TopologyKeyMask;
import me.ilyamirin.anthophila.common.Topology;

import java.nio.ByteBuffer;
import java.util.Set;

import static org.junit.Assert.*;


/**
 *
 * @author ilyamirin
 */
@Slf4j
public class TopologyTest {

    @Test
    public void testTopology() {
        Set<TopologyNode> nodesForFirst = Sets.newHashSet(new TopologyNode("127.0.0.1", 999), new TopologyNode("127.0.0.4", 999));

        TopologyKeyMask first = new TopologyKeyMask();
        first.setMask(new byte[]{ 0 });
        first.setNodes(nodesForFirst);

        Set<TopologyNode> nodesForSecond = Sets.newHashSet(new TopologyNode("127.0.0.2", 999));

        TopologyKeyMask second = new TopologyKeyMask();
        second.setMask(new byte[]{ 1, 0 });
        second.setNodes(nodesForSecond);

        Set<TopologyNode> nodesForThird = Sets.newHashSet(new TopologyNode("127.0.0.3", 999));

        TopologyKeyMask third = new TopologyKeyMask();
        third.setMask(new byte[]{ 1, 1 });
        third.setNodes(nodesForThird);

        Topology topology = new Topology();
        topology.setKeyMasks(Sets.newHashSet(first, second, third));

        byte[] keyForFirstNode = new byte[] { 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 };
        assertTrue(Sets.symmetricDifference(topology.findNodes(ByteBuffer.wrap(keyForFirstNode)), nodesForFirst).isEmpty());

        byte[] keyForSecondNode = new byte[] { 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 };
        assertTrue(Sets.symmetricDifference(topology.findNodes(ByteBuffer.wrap(keyForSecondNode)), nodesForSecond).isEmpty());

        byte[] keyForThirdNode = new byte[] { 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0 };
        assertTrue(Sets.symmetricDifference(topology.findNodes(ByteBuffer.wrap(keyForThirdNode)), nodesForThird).isEmpty());
    }//testEnigma
}
