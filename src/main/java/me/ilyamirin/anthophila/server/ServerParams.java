package me.ilyamirin.anthophila.server;

import com.google.gson.Gson;
import java.io.FileNotFoundException;
import java.io.FileReader;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
public class ServerParams {

    private static final Gson GSON = new Gson();
    private String pathToStorageFile;
    private String host;
    private int port;
    private int maxPendingConnections;

    public static ServerParams loadFormFile(String filename) throws FileNotFoundException {
        FileReader fileReader = new FileReader(filename);
        return GSON.fromJson(fileReader, ServerParams.class);
    }
}
