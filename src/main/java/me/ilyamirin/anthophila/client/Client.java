package me.ilyamirin.anthophila.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.http.HttpClientFilter;
import org.glassfish.grizzly.impl.FutureImpl;
import org.glassfish.grizzly.impl.SafeFutureImpl;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@RequiredArgsConstructor
public class Client {

    @NonNull
    private String host;
    @NonNull
    private int port;
    @NonNull
    private long timeoutInSeconds;
    private TCPNIOTransport transport;
    private FutureImpl<Map<Long, Boolean>> completeFuture = SafeFutureImpl.create();

    public void init() throws IOException {
        FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
        clientFilterChainBuilder.add(new TransportFilter());
//        clientFilterChainBuilder.add(new IdleTimeoutFilter(new DelayedExecutor(new FixedThreadPool, timeoutInSeconds, TimeUnit.SECONDS), timeoutInSeconds, TimeUnit.SECONDS));
        clientFilterChainBuilder.add(new HttpClientFilter());
        clientFilterChainBuilder.add(new ChunksPushingFilter(host, completeFuture));

        transport = TCPNIOTransportBuilder.newInstance().build();
        transport.setProcessor(clientFilterChainBuilder.build());
        transport.start();
    }

    public Map<Long, Boolean> sendChunks(Map<ByteBuffer, ByteBuffer> hashesWithChunks) throws IOException {
        Connection connection = null;
        Future<Connection> connectFuture = transport.connect(host, port);

        try {
            connection = connectFuture.get(timeoutInSeconds, TimeUnit.SECONDS);
            return completeFuture.get();

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (connection == null) {
                log.error("Can not connect to the server {}:{} resource", host, port);
            } else {
                log.error("Error pushing the chunks map");
            }
            return null;

        } finally {
            if (connection != null) {
                connection.close();
            }

        }
    }

    public void close() throws IOException {
        transport.stop();
    }
}
