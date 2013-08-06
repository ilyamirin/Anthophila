package me.ilyamirin.anthophila;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
@AllArgsConstructor
public class IndexEntry {

    private long chunkPosition;
    private int chunkLength;
}
