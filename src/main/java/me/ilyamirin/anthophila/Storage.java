package me.ilyamirin.anthophila;

/**
 *
 * @author ilyamirin
 */
public interface Storage {

    boolean contains(byte[] md5Hash);

    void append(byte[] md5Hash, byte[] chunk);

    byte[] read(byte[] md5Hash);
}
