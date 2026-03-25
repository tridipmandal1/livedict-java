package com.livedict.backend;

import com.livedict.serialization.KryoSerializer;
import com.livedict.serialization.SerializationException;
import com.livedict.serialization.Serializer;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Redis-backed persistence using Lettuce client and Kryo serialization.
 *
 * <p><b>Features:</b>
 * <ul>
 *   <li>Distributed caching — multiple LiveDict instances share the same Redis</li>
 *   <li>Kryo serialization for compact, fast encoding</li>
 *   <li>Native TTL support via Redis PSETEX/EXPIRE commands</li>
 *   <li>Pipelined batch operations for efficiency</li>
 *   <li>Key isolation — uses prefix to avoid conflicts with other Redis users</li>
 * </ul>
 *
 * <p><b>Key prefix:</b> All keys are prefixed with "livedict:" by default to
 * isolate LiveDict data from other Redis users. This can be customized via
 * constructor parameter.
 *
 * <p><b>Connection management:</b> Lettuce connections are thread-safe and
 * multiplexed over a single TCP connection. No connection pool needed.
 *
 * <p><b>Example:</b>
 * <pre>{@code
 *   RedisClient redisClient = RedisClient.create("redis://localhost:6379");
 *   RedisBackend<String, User> backend = new RedisBackend<>(
 *       redisClient,
 *       "myapp:",  // custom prefix
 *       new KryoSerializer<>(),
 *       new KryoSerializer<>()
 *   );
 *
 *   LiveDictConfig config = LiveDictConfig.builder()
 *       .backend(backend)
 *       .build();
 * }</pre>
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */

public class RedisBackend<K, V> implements Backend<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisBackend.class);
    private static final String DEFAULT_KEY_PREFIX = "livedict:";

    private final RedisClient client;

    private final StatefulRedisConnection<byte[], byte[]> connection;

    private final RedisCommands<byte[], byte[]> sync;

    private final RedisAsyncCommands<byte[], byte[]> async;

    private final String keyPrefix;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    /**
     * Creates a Redis backend with Kryo serialization and default key prefix.
     *
     * @param redisClient the Lettuce Redis client (e.g., {@code RedisClient.create("redis://localhost:6379")})
     */
    public RedisBackend(RedisClient redisClient) {
        this(redisClient,DEFAULT_KEY_PREFIX, new KryoSerializer<>(), new KryoSerializer<>());
    }

    /**
     * Creates a Redis backend with custom key prefix and Kryo serialization.
     *
     * @param redisClient the Lettuce Redis client
     * @param keyPrefix   the prefix for all Redis keys (e.g., "myapp:")
     */
    public RedisBackend(RedisClient redisClient, String keyPrefix) {
        this(redisClient, keyPrefix, new KryoSerializer<>(), new KryoSerializer<>());
    }

    /**
     * Creates a Redis backend with custom serializers and key prefix.
     *
     * @param redisClient      the Lettuce Redis client
     * @param keyPrefix        the prefix for all Redis keys (e.g., "livedict:")
     * @param keySerializer    the serializer for keys
     * @param valueSerializer  the serializer for values
     */
    public RedisBackend(RedisClient redisClient,
                        String keyPrefix,
                        Serializer<K> keySerializer,
                        Serializer<V> valueSerializer) {
        this.client = redisClient;
        this.connection = redisClient.connect(ByteArrayCodec.INSTANCE);
        this.sync = connection.sync();
        this.async = connection.async();
        this.keyPrefix = keyPrefix;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        LOGGER.info("Redis backend initialized with prefix: {}", keyPrefix);
    }

    /**
     * Prefixes a serialized key with the configured key prefix.
     */
    private byte[] prefixKey(byte[] keyBytes) {
        byte[] prefixBytes = keyPrefix.getBytes();
        byte[] result = new byte[prefixBytes.length + keyBytes.length];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(keyBytes, 0, result, prefixBytes.length, keyBytes.length);
        return result;
    }

    /**
     * Removes the prefix from a Redis key to get the original serialized key.
     */
    private byte[] unprefixKey(byte[] redisKey) {
        byte[] prefixBytes = keyPrefix.getBytes();
        byte[] result = new byte[redisKey.length - prefixBytes.length];
        System.arraycopy(redisKey, prefixBytes.length, result, 0, result.length);
        return result;
    }

    @Override
    public void put(K key, V value, long expiresAt) throws BackendException {

        try {
            byte[] keyBytes = keySerializer.serialize(key);
            byte[] redisKey = prefixKey(keyBytes);
            byte[] valueBytes = valueSerializer.serialize(value);

            if (expiresAt > 0) {
                // calculate TTL in seconds
                long now = System.currentTimeMillis();
                long ttlMillis = Math.max(1, (expiresAt - now));

                // using PSETEX to ensure atomicity with TTL
                sync.psetex(redisKey, ttlMillis, valueBytes);
            } else {
                sync.set(redisKey, valueBytes);
            }
        } catch (SerializationException e) {
            throw new BackendException("Failed to serialize for Redis put", e);
        }
    }

    @Override
    public V get(K key) throws BackendException {

        try {
            byte[] keyBytes= keySerializer.serialize(key);
            byte[] redisKey = prefixKey(keyBytes);
            byte[] valueBytes = sync.get(redisKey);

            if (valueBytes == null) return null;

            return valueSerializer.deserialize(valueBytes);
        } catch (SerializationException e) {
            throw new BackendException("Failed to deserialize from Redis get", e);
        }
    }

    @Override
    public void delete(K key) throws BackendException {

        try {
            byte[] keyBytes = keySerializer.serialize(key);
            byte[] redisKey = prefixKey(keyBytes);
            sync.del(redisKey);
        } catch (SerializationException e) {
            throw new BackendException("Failed to serialize for Redis delete", e);
        }
    }

    @Override
    public Map<K, V> loadAll() throws BackendException {

        Map<K, V> result = new HashMap<>();

        try {
            // Get all keys from Redis with cursor pagination
            String pattern = keyPrefix + "*";
            ScanCursor cursor = ScanCursor.INITIAL;
            ScanArgs scanArgs  = ScanArgs.Builder.limit(100).match(pattern);

            do {
                KeyScanCursor<byte[]> scanResult = sync.scan(cursor, scanArgs);
                List<byte[]> keys = scanResult.getKeys();

                for (byte[] redisKey: keys) {
                    byte[] valueBytes = sync.get(redisKey);
                    if (valueBytes != null) {
                        byte[] keyBytes = unprefixKey(redisKey);
                        K key = keySerializer.deserialize(keyBytes);
                        V value = valueSerializer.deserialize(valueBytes);
                        result.put(key, value);
                    }
                }
                cursor = scanResult;
            } while (!cursor.isFinished());
            LOGGER.info("Loaded {} entries from Redis", result.size());
            return result;
        } catch (SerializationException e) {
            throw new BackendException("Failed to deserialize from Redis loadAll", e);
        }
    }

    @Override
    public void clear() throws BackendException {
        LOGGER.info("Clearing all keys with prefix: {}", keyPrefix);

        try {
            String pattern = keyPrefix + "*";
            ScanCursor cursor = ScanCursor.INITIAL;
            ScanArgs scanArgs  = ScanArgs.Builder.limit(100).match(pattern);

            int deletedCount = 0;

            do {
                KeyScanCursor<byte[]> scanResult = sync.scan(cursor, scanArgs);
                List<byte[]> keys = scanResult.getKeys();

                if (!keys.isEmpty()) {
                    byte[][] keysArray = keys.toArray(new byte[0][]);
                    sync.del(keysArray);
                    deletedCount += keys.size();
                }
                cursor = scanResult;
            } while (!cursor.isFinished());
            LOGGER.info("Deleted {} keys from Redis", deletedCount);
        } catch (Exception e) {
            throw new BackendException("Redis clear failed", e);
        }
    }

    @Override
    public void close() throws BackendException {
        connection.close();
        client.shutdown();
        LOGGER.info("RedisBackend closed");
    }

    // Batch operations - optimized pipelining
    @Override
    public void putBatch(List<PutEntry<K, V>> putEntries) throws BackendException {
        if (putEntries.isEmpty()) {
            return;
        }

        try {
            // disable auto flush  to enable pipelining
            connection.setAutoFlushCommands(false);

            List<RedisFuture<?>> futures = new ArrayList<>();

            for(PutEntry<K, V> entry: putEntries) {
                byte[] keyBytes = keySerializer.serialize(entry.key());
                byte[] redisKey = prefixKey(keyBytes);
                byte[] valueBytes = valueSerializer.serialize(entry.value());

                if (entry.expiresAt() > 0) {
                    long now = System.currentTimeMillis();
                    long ttlMillis = Math.max(1, (entry.expiresAt() - now));
                    futures.add(async.psetex(redisKey, ttlMillis, valueBytes));
                } else {
                    futures.add(async.set(redisKey, valueBytes));
                }
            }
            // flush all commands in the pipeline
            connection.flushCommands();

            for (RedisFuture<?> future : futures) {
                future.get();
            }

            connection.setAutoFlushCommands(true);

            LOGGER.info("Batch inserted {} entries via Redis pipeline", putEntries.size());
        } catch (SerializationException | InterruptedException | ExecutionException e) {
            connection.setAutoFlushCommands(true);  // restore on error
            throw new BackendException("Redis batch put failed", e);
        }
    }

    @Override
    public void deleteBatch(List<K> keys) throws BackendException {
        if (keys.isEmpty()) {
            return;
        }

        try {
            connection.setAutoFlushCommands(false);

            List<RedisFuture<?>> futures = new ArrayList<>();

            for (K key: keys) {
                byte[] keyBytes = keySerializer.serialize(key);
                byte[] redisKey = prefixKey(keyBytes);
                futures.add(async.del(redisKey));
            }
            connection.flushCommands();

            for (RedisFuture<?> future : futures) {
                future.get();
            }

            connection.setAutoFlushCommands(true);
            LOGGER.info("Batch deleted {} entries via Redis pipeline", keys.size());
        } catch (SerializationException | InterruptedException | ExecutionException e) {
            connection.setAutoFlushCommands(true);
            throw new BackendException("Redis batch delete failed", e);
        }
    }
}
