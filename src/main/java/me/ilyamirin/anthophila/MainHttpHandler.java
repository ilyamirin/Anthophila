package me.ilyamirin.anthophila;

import java.nio.ByteBuffer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.http.io.NIOInputStream;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.memory.ByteBufferWrapper;

/**
 *
 * @author ilyamirin
 */
@RequiredArgsConstructor
public class MainHttpHandler extends HttpHandler {

    @NonNull
    private Storage storage;

    @Override
    public void service(Request request, Response response) throws Exception {
        NIOInputStream inputStream = request.getNIOInputStream();

        if (request.getMethod().matchesMethod("HEAD")) {
            if (storage.contains(inputStream.readBuffer(8).toByteBuffer())) {
                response.setStatus(HttpStatus.FOUND_302);
            } else {
                response.setStatus(HttpStatus.NOT_FOUND_404);
            }

        } else if (request.getMethod().matchesMethod("POST")) {
            while (inputStream.isReady()) {
                ByteBuffer chunkMd5Hash = inputStream.readBuffer(8).toByteBuffer();
                if (!storage.contains(chunkMd5Hash)) {
                    int chunkLength = inputStream.readBuffer(4).getInt(0);
                    ByteBuffer chunk = inputStream.readBuffer(chunkLength).toByteBuffer();
                    storage.append(chunkMd5Hash, chunk);
                }
            }
            response.setStatus(HttpStatus.CREATED_201);

        } else if (request.getMethod().matchesMethod("GET")) {
            long contextLength = 0;
            while (inputStream.isReady()) {
                ByteBuffer chunkMd5Hash = inputStream.readBuffer(8).toByteBuffer();
                ByteBuffer chunk = storage.read(chunkMd5Hash);
                if (chunk != null) {
                    contextLength += chunk.capacity();
                    Buffer buffer = new ByteBufferWrapper(chunk);
                    response.getNIOOutputStream().write(buffer);
                }
            }
            response.setContentLengthLong(contextLength);
            response.setStatus(HttpStatus.ACCEPTED_202);

        } else {
            response.setStatus(HttpStatus.BAD_REQUEST_400);
        }
    }
}
