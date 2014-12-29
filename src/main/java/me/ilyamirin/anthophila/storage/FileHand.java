package me.ilyamirin.anthophila.storage;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.ilyamirin.anthophila.BufferUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by ilyamirin on 29.12.14.
 */
@Slf4j
@AllArgsConstructor
public class FileHand implements Hand {

    private FileChannel fileChannel;

    public static FileHand create(String pathToFile) throws FileNotFoundException {
        FileChannel channel = new RandomAccessFile(pathToFile, "rw").getChannel();
        return new FileHand(channel);
    }

    @Override
    public boolean write(int position, ByteBuffer bufferToWrite) {
        try {
            while (bufferToWrite.hasRemaining())
                fileChannel.write(bufferToWrite, position);
            bufferToWrite.rewind();
            return true;
        } catch (IOException e) {
            log.error("Error caught during buffer writing:", e);
            return false;
        }
    }

    @Override
    public ByteBuffer read(int position, int size) {
        try {
            ByteBuffer bufferToRead = BufferUtils.emptyBuffer(size);
            while (bufferToRead.hasRemaining())
                fileChannel.read(bufferToRead, position);
            bufferToRead.clear();
            return bufferToRead;
        } catch (IOException e) {
            log.error("Error caught during buffer reading:", e);
            return null;
        }
    }

}
