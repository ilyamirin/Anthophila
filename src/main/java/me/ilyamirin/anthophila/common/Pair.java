package me.ilyamirin.anthophila.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
@AllArgsConstructor
public class Pair<T, E> {

    public static <T, E> Pair<T, E> newPair(T key, E value) {
        return new Pair(key, value);
    }
    
    private T key;
    private E value;
}
