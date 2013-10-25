package me.ilyamirin.anthophila.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 *
 * @author ilyamirin
 */
@Data
@AllArgsConstructor
public class Node {
    
    private String host;
    private int port;    
}
