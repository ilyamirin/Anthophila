package me.ilyamirin.anthophila.indexes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.ilyamirin.anthophila.hands.Hand;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexEntry {

    private Hand hand;
    private Integer position;
    private Integer size;
}
