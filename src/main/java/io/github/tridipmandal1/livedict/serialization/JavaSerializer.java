package io.github.tridipmandal1.livedict.serialization;

import java.io.*;


/**
 * Java's built-in serialization using {@link ObjectOutputStream}.
 *
 * <p><b>Performance:</b> Baseline (1x speed, 1x size). Slower and larger than Kryo.
 *
 * <p><b>Use case:</b> Backward compatibility with v1.1 SqliteBackend, or when
 * you need Java's standard serialization versioning mechanisms.
 *
 * <p><b>Requirements:</b> Objects must implement {@link Serializable}.
 *
 * <p><b>When to use:</b> Only when you need exact compatibility with v1.1
 * serialized data, or when working with Java serialization-specific features
 * like {@code serialVersionUID}.
 *
 * @param <T> the type of objects to serialize — must implement Serializable
 */
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
