package me.ilyamirin.anthophila.hands;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by ilyamirin on 29.12.14.
 */
@Slf4j
public class FileHandTest extends HandTest {

    @Before
    public void init() throws FileNotFoundException {
        writer = FileHand.create("filehandtestFile");
    }

    @Test
    public void test() throws IOException{
        innerTest();
    }

    @After
    public void clean() throws FileNotFoundException {
        new File("filehandtestFile").delete();
    }
}
