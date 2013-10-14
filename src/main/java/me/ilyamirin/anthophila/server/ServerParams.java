package me.ilyamirin.anthophila.server;

import lombok.Data;

/**
 * @author ilyamirin
 */
@Data
public class ServerParams {

    private String storageFile;

    private int initialIndexSize;

    private String host;
    private int port;
    private int maxConnections;

    private boolean isEncrypt;
    private String newKeysFile;
    private String oldKeysFile;

    private boolean serveAll;
    private String topologyFile;
}
