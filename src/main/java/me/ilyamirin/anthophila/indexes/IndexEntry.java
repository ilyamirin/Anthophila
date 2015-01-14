package me.ilyamirin.anthophila.indexes;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by ilyamirin on 14.01.15.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexEntry {

    private String handName;
    private Integer position;
    private Integer size;
}
