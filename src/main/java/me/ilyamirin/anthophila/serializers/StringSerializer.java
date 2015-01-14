package me.ilyamirin.anthophila.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public ByteBuffer serialize(String target) {
        return ByteBuffer.wrap(target.getBytes(Charset.forName("UTF-8")));
    }

    @Override
    public String deserialize(ByteBuffer target) {
        return new String(target.array(), Charset.forName("UTF-8"));
    }
}
