package me.ilyamirin.anthophila.hands;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by ilyamirin on 15.01.15.
 */
public class InMemoryHandTest extends HandTest {

    @Before
    public void init() throws FileNotFoundException {
        writer = new InMemoryHand();
    }

    @Test
    public void test() throws IOException {
        innerTest();
    }
}
