package me.ilyamirin.anthophila.client;

import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import me.ilyamirin.anthophila.common.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ilyamirin
 * Date: 15.10.13
 * Time: 15:52
 * To change this template use File | Settings | File Templates.
 */
@RequiredArgsConstructor
public class ClusterClient implements Client {

    @NonNull
    private Map<Topology.Node, OneNodeClient> clients;
    @NonNull
    private Topology topology;

    public static ClusterClient newClusterClient(Topology topology) throws IOException {
        Map<Topology.Node, OneNodeClient> clients = Maps.newHashMap();
        for (Topology.Node node : topology.getNodes().keySet())
            clients.put(node, OneNodeClient.newClient(node.getHost(), node.getPort()));
        return new ClusterClient(clients, topology);
    }

    @Override
    public boolean push(ByteBuffer key, ByteBuffer chunk) throws IOException {
        for (Topology.Node node : topology.findNodes(key)) {
            if (clients.containsKey(node))
                return clients.get(node).push(key, chunk);
        }
        return false;
    }

    @Override
    public ByteBuffer pull(ByteBuffer key) throws IOException {
        for (Topology.Node node : topology.findNodes(key)) {
            if (clients.containsKey(node))
                return clients.get(node).pull(key);
        }
        return null;
    }

    @Override
    public boolean remove(ByteBuffer key) throws IOException {
        for (Topology.Node node : topology.findNodes(key)) {
            if (clients.containsKey(node))
                return clients.get(node).remove(key);
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        for (OneNodeClient client : clients.values())
            client.close();
    }
}
