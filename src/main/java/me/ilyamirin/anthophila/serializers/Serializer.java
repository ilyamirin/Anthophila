package me.ilyamirin.anthophila.serializers;

import java.nio.ByteBuffer;

/**
 * Created by ilyamirin on 14.01.15.
 */
public interface Serializer <T> {

    ByteBuffer serialize(T target);

    T deserialize(ByteBuffer target);
}
