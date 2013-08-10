package me.ilyamirin.anthophila.server;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.glassfish.grizzly.http.server.HttpServer;

@Slf4j
public class Server {

    public static final String CURRENT_PROTOCOL_VERSION = "/v1";

    public class ServerParams {
        public static final String PATH_TO_STORAGE = "--pathToStorage";
        public static final String HOST = "--host";
        public static final String PORT = "--port";
    }

    private HttpServer server;

    public void start(Map<String, Object> params) throws FileNotFoundException {
        String pathToStorage = (String) params.get(ServerParams.PATH_TO_STORAGE);
        RandomAccessFile accessFile = new RandomAccessFile(pathToStorage, "rw");
        Storage storage = new StorageImpl(accessFile.getChannel());

        String host = (String) params.get(ServerParams.HOST);
        Integer port = (Integer) params.get(ServerParams.PORT);
        server = HttpServer.createSimpleServer("/", host, port);
        
        server.getServerConfiguration().addHttpHandler(new MainHttpHandler(storage), CURRENT_PROTOCOL_VERSION);

        try {
            server.start();
            //System.out.println("Press any key to stop the server...");
            //System.in.read();
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    public void stop() {
        server.shutdown();
    }

    public static void main(String[] args) {

        for (int i = 0; i < args.length; i += 2) {
            String[] param = Arrays.copyOfRange(args, i, i + 2);
            if (param[0].equals("--pathToStorage")) {
                //accessFile
            }
        }

    }
}
