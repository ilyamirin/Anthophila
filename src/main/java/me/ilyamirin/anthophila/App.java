package me.ilyamirin.anthophila;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.http.io.NIOInputStream;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;

@Slf4j
public class App {

    public static void main(String[] args) throws FileNotFoundException {
        final Storage storage = new StorageImpl(new RandomAccessFile("base.bin", "rw").getChannel());

        HttpServer server = HttpServer.createSimpleServer();
        server.getServerConfiguration().addHttpHandler(new MainHttpHandler(storage), "/chunk");

        try {
            server.start();
            System.out.println("Press any key to stop the server...");
            System.in.read();
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
