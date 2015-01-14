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
    public void write(int position, ByteBuffer bufferToWrite) throws IOException {
        while (bufferToWrite.hasRemaining())
            fileChannel.write(bufferToWrite, position);
        bufferToWrite.rewind();
    }

    @Override
    public ByteBuffer read(int position, int size) throws IOException {
        ByteBuffer bufferToRead = BufferUtils.emptyBuffer(size);
        while (bufferToRead.hasRemaining())
            fileChannel.read(bufferToRead, position);
        bufferToRead.clear();
        return bufferToRead;
    }

}
