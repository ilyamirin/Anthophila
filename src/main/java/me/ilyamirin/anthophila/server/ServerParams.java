package me.ilyamirin.anthophila.server;

import com.beust.jcommander.Parameter;
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

    @Parameter(names = "--storage", description = "Path to storage file.")
    private String pathToStorageFile;
    private String host = "localhost";
    @Parameter(names = "--port", description = "On which port shoudl server connect?")
    private int port;
    @Parameter(names = "--max-connections", description = "How many connections is available to connect simultaneously.")
    private int maxPendingConnections;
    @Parameter(names = "--encryption", description = "Is encrypt new chunks.")
    private boolean isEncrypted;
}
