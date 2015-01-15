package me.ilyamirin.anthophila.hands;

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

    private final FileChannel fileChannel;

    public static FileHand create(String pathToFile) throws FileNotFoundException {
        FileChannel channel = new RandomAccessFile(pathToFile, "rw").getChannel();
        return new FileHand(channel);
    }

    @Override
    public long size() throws IOException {
        return fileChannel.size();
    }

    @Override
    public void write(Long position, ByteBuffer bufferToWrite) throws IOException {
        synchronized (this) {
            while (bufferToWrite.hasRemaining())
                fileChannel.write(bufferToWrite, position.intValue());
        }
        bufferToWrite.rewind();
    }

    @Override
    public ByteBuffer read(Long position, int size) throws IOException {
        ByteBuffer bufferToRead = BufferUtils.emptyBuffer(size);
        synchronized (this) {
            while (bufferToRead.hasRemaining())
                fileChannel.read(bufferToRead, position.intValue());
        }
        bufferToRead.clear();
        return bufferToRead;
    }

}
