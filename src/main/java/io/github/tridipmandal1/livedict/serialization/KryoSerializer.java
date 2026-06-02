package io.github.tridipmandal1.livedict.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;


/**
 * Fast, compact serialization using Kryo.
 *
 * <p><b>Performance:</b> ~10x faster than Java serialization, ~70% smaller output.
 *
 * <p><b>Thread safety:</b> Uses ThreadLocal to provide one Kryo instance per thread.
 * This is the recommended Kryo usage pattern for multi-threaded environments.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>No {@code Serializable} interface required</li>
 *   <li>Supports classes without no-arg constructors via Objenesis</li>
 *   <li>Handles circular references and complex object graphs</li>
 *   <li>Auto-registers classes dynamically</li>
 * </ul>
 *
 * <p><b>Limitations:</b>
 * <ul>
 *   <li>Java-only (not cross-language compatible)</li>
 *   <li>Schema evolution limited compared to Protobuf</li>
 * </ul>
 *
 * <p><b>When to use:</b> High-performance Java-only applications where serialization
 * speed and size matter (SQLite backends, Redis backends with high write throughput).
 *
 * @param <T> the type of objects to serialize
 */
public class KryoSerializer<T> implements Serializer<T>{

    /**
     * ThreadLocal Kryo instance.
     *
     * <p>Kryo is NOT thread-safe, so we use ThreadLocal to give each thread
     * its own instance.
     *
     * <p>Configured with StdInstantiatorStrategy to handle classes without
     * no-arg constructors (POJOs, records, local classes).
     */
    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        // Don't require registration — auto-register classes as encountered
        kryo.setRegistrationRequired(false);
        // Allow references to handle circular dependencies
        kryo.setReferences(true);
         // Use Objenesis to instantiate classes without no-arg constructors
          // This is essential for deserializing POJOs, records, and local classes
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    });

    @Override
    public byte[] serialize(T obj) throws SerializationException {
        if (obj == null) {
            return new byte[0];
        }

        try {
            Kryo kryo = kryoThreadLocal.get();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output output = new Output(baos);
            kryo.writeClassAndObject(output, obj);
            output.close();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Kryo serialization failed for object: " + obj.getClass().getName(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        try {
            Kryo kryo = kryoThreadLocal.get();
            Input input = new Input(bytes);
            Object result = kryo.readClassAndObject(input);
            input.close();
            return (T) result;
        } catch (Exception e) {
            throw new SerializationException("Kryo deserialization failed", e);
        }
    }
}
