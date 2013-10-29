package me.ilyamirin.anthophila.client;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.common.Node;
import me.ilyamirin.anthophila.common.Topology;
import me.ilyamirin.anthophila.server.ServerParams;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@AllArgsConstructor
public class ReplicationClient implements Client {

    private Set<OneNodeClient> clients;

    public static ReplicationClient newReplicationClient(ServerParams params, Topology topology) throws IOException {
        Set<OneNodeClient> clients = Sets.newHashSet();
        Node thisNode = new Node(params.getHost(), params.getPort());
        Set<List<Byte>> masks = topology.getNodes().get(thisNode);
        for (List<Byte> mask : masks) {
            for (Node node : topology.getKeyMasks().get(mask)) {
                if (!node.equals(thisNode)) {
                    clients.add(OneNodeClient.newClient(node.getHost(), node.getPort()));
                }
            }
        }
        return new ReplicationClient(clients);
    }
    
    @Override
    public boolean push(ByteBuffer key, ByteBuffer chunk) throws IOException {
        boolean result = true;
        for (OneNodeClient client : clients) {
            result &= client.push(key, chunk);
        }
        return result;
    }

    @Override
    public ByteBuffer pull(ByteBuffer key) throws IOException {
        ByteBuffer result = null;
        for (OneNodeClient client : clients) {
            ByteBuffer pulled = client.pull(key);
            if (result == null) {
                result = pulled;
            } else if (!result.equals(pulled)) {
                return null;
            }
        }
        return result;
    }

    @Override
    public boolean remove(ByteBuffer key) throws IOException {
        boolean result = true;
        for (OneNodeClient client : clients) {
            result &= client.remove(key);
        } 
        return result;
    }

    @Override
    public boolean seek(ByteBuffer key) throws IOException {
        boolean result = true;
        for (OneNodeClient client : clients) {
            result &= client.seek(key);
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        for (OneNodeClient client : clients) {
            client.close();
        }
    }

}
