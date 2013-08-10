package me.ilyamirin.anthophila.client;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.server.Server;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.Protocol;
import org.glassfish.grizzly.impl.FutureImpl;

/**
 *
 * @author ilyamirin
 */
@Slf4j
@RequiredArgsConstructor
public class ChunksPushingFilter extends BaseFilter {

    @NonNull
    private final String host;
    @NonNull
    private FutureImpl<Map<Long, Byte>> completeFuture;
    //private volatile FileChannel output;
    //private volatile int bytesDownloaded;
    @Setter
    private ByteBuffer byteBuffer;

    @Override
    public NextAction handleConnect(FilterChainContext ctx) throws IOException {
        final HttpRequestPacket httpRequest = HttpRequestPacket
                .builder()
                .method(Method.POST)
                .uri(Server.CURRENT_PROTOCOL_VERSION)
                .protocol(Protocol.HTTP_1_1)
                .header("Host", host)
                .chunked(true)
                .build();

        ctx.setMessage(byteBuffer);

        ctx.write(httpRequest);

        return ctx.getStopAction();
    }

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        try {
            final HttpContent httpContent = (HttpContent) ctx.getMessage();
            final Buffer buffer = httpContent.getContent();

            Map<Long, Byte> result = Maps.newHashMap();

            if (buffer.remaining() > 0) {
                ByteBuffer byteBuffer = buffer.toByteBuffer();
                int position = 0;
                while (byteBuffer.hasRemaining() && position < byteBuffer.capacity()) {
                    long md5Hash = byteBuffer.getLong(position);
                    position += 8;
                    byte resultForChunk = byteBuffer.get(position);
                    position++;
                    result.put(md5Hash, resultForChunk);
                }
                buffer.dispose();
            }

            if (httpContent.isLast()) {
                completeFuture.result(result);
                close();
            }
        } catch (IOException e) {
            close();
        }

        return ctx.getStopAction();
    }

    @Override
    public NextAction handleClose(FilterChainContext ctx) throws IOException {
        close();
        return ctx.getStopAction();
    }

    private void close() throws IOException {
        if (!completeFuture.isDone()) {
            completeFuture.failure(new IOException("Connection was closed"));
        }
    }
}
