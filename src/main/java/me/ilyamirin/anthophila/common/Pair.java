package me.ilyamirin.anthophila.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
@AllArgsConstructor
public class Pair {

    private byte[] key;
    private byte[] value;
}
