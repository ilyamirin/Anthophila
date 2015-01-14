package me.ilyamirin.anthophila.serializers;

import org.junit.Test;

/**
 * Created by ilyamirin on 14.01.15.
 */
public class StringSerializerTest {

    @Test
    public void test() {
        Serializer<String> serializer = new StringSerializer();
        String string = "TEST string ывплотытап 134234 \";:Н№:*%;\"?№";
        assert serializer.deserialize(serializer.serialize(string)).equals(string);
    }
}
