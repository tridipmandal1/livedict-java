package io.github.tridipmandal1.livedict.serialization;

/**
 * Abstraction for serializing objects to/from byte arrays.
 *
 * <p>Implementations define how LiveDict persists keys and values to backends.
 * Different serializers offer different trade-offs:
 *
 * <table>
 *   <tr>
 *     <th>Serializer</th>
 *     <th>Speed</th>
 *     <th>Size</th>
 *     <th>Cross-Language</th>
 *     <th>Use Case</th>
 *   </tr>
 *   <tr>
 *     <td>{@link KryoSerializer}</td>
 *     <td>Fast (10x)</td>
 *     <td>Small (0.3x)</td>
 *     <td>No</td>
 *     <td>Java-only, performance-critical</td>
 *   </tr>
 *   <tr>
 *     <td>{@link JavaSerializer}</td>
 *     <td>Slow (1x)</td>
 *     <td>Large (1x)</td>
 *     <td>No</td>
 *     <td>Backward compatibility</td>
 *   </tr>
 * </table>
 *
 * <p><b>Thread safety:</b> Implementations must be thread-safe.
 *
 * <p><b>Example usage:</b>
 * <pre>{@code
 *   Serializer<User> serializer = new KryoSerializer<>();
 *   byte[] bytes = serializer.serialize(user);
 *   User restored = serializer.deserialize(bytes);
 * }</pre>
 *
 * @param <T> the type of objects this serializer handles
 */
public interface Serializer<T> {

    /**
     * Serializes an object to a byte array.
     *
     * @param obj the object to serialize (may be null if the serializer supports it)
     * @return the serialized bytes
     * @throws SerializationException if serialization fails
     */
    byte[] serialize(T obj) throws SerializationException;


    /**
     * Deserializes a byte array back to an object.
     *
     * @param bytes the bytes to deserialize
     * @return the deserialized object
     * @throws SerializationException if deserialization fails
     */
    T deserialize(byte[] bytes) throws SerializationException;
}
