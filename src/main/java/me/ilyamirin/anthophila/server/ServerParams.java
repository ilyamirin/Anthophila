package me.ilyamirin.anthophila.server;

import com.beust.jcommander.Parameter;
import lombok.Data;

/**
 * @author ilyamirin
 */
@Data
public class ServerParams {

    @Parameter(names = "--storage", description = "Where is storage file located?")
    private String pathToStorageFile;
    @Parameter(names = "--host", description = "On which host should server bind?")
    private String host = "localhost";
    @Parameter(names = "--port", description = "On which port should server bind?")
    private int port;
    @Parameter(names = "--maxConnections", description = "How many connections should should server pend simultaneously?")
    private int maxPendingConnections;
    @Parameter(names = "--encryption", description = "Should server encrypt new chunks?")
    private boolean isEncrypted;
}
