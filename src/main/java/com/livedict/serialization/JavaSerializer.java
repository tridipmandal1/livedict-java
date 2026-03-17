package com.livedict.serialization;

import java.io.*;

public class JavaSerializer<T> implements Serializer<T>{


    @Override
    public byte[] serialize(T obj) throws SerializationException {
        if (obj == null) {
            return new byte[0];
        }

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Java serialization failed for object: " + obj.getClass().getName(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object result = ois.readObject();
            ois.close();
            return (T) result;
        } catch (IOException | ClassNotFoundException e) {
            throw new SerializationException("Java deserialization failed", e);
        }
    }
}
