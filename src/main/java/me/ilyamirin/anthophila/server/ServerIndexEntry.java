package me.ilyamirin.anthophila.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
@AllArgsConstructor
public class ServerIndexEntry {

    private int chunkPosition;
    private int chunkLength;
}
