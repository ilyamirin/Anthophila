package me.ilyamirin.anthophila.server;

import lombok.Data;

/**
 * @author ilyamirin
 */
@Data
public class ServerParams {

    private String target;

    private int maxExpectedSize;

    private String host;
    private int port;
    private int maxConnections;

    private boolean isEncrypt;
    private String actualKeysFile;
    private String oldKeysFile;

    private boolean serveAll;

    private String topologyFile;
}
