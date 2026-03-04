package com.livedict.backend;

import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SQLiteBackend<K, V> implements Backend<K, V>{

    private static final Logger LOGGER = Logger.getLogger(SQLiteBackend.class.getName());

    private final String dbPath;
    private final Connection connection;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final PreparedStatement putStmt;
    private final PreparedStatement getStmt;
    private final PreparedStatement deleteStmt;
    private final PreparedStatement clearStmt;

    public SQLiteBackend(String dbPath) {
        this.dbPath = dbPath;

        try {
            String url = "jdbc:sqlite:" + dbPath;
            this.connection = DriverManager.getConnection(url);
            this.connection.setAutoCommit(true);

            initSchema();

            this.putStmt = connection.prepareStatement(
                    "INSERT OR REPLACE INTO livedict_entries " +
                            "(key_hash, key_bytes, value_bytes, expires_at, created_at) " +
                            "VALUES (?, ?, ?, ?, ?)"
            );
            this.getStmt = connection.prepareStatement(
                    "SELECT value_bytes, expires_at FROM livedict_entries WHERE key_hash = ?"
            );
            this.deleteStmt = connection.prepareStatement(
                    "DELETE FROM livedict_entries WHERE key_hash = ?"
            );
            this.clearStmt = connection.prepareStatement(
                    "DELETE FROM livedict_entries"
            );

            startCleanupTask();
            LOGGER.info("SqliteBackend initialized at: " + dbPath);
        } catch (SQLException e) {
            throw new BackendException("Failed to initialize SQLite backend", e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    "CREATE TABLE IF NOT EXISTS livedict_entries (" +
                            "  key_hash   TEXT PRIMARY KEY," +
                            "  key_bytes  BLOB NOT NULL," +
                            "  value_bytes BLOB NOT NULL," +
                            "  expires_at BIGINT," +
                            "  created_at BIGINT NOT NULL" +
                            ")"
            );
            stmt.execute(
                    "CREATE INDEX IF NOT EXISTS idx_expires " +
                            "ON livedict_entries(expires_at) " +
                            "WHERE expires_at != -1"
            );
        }
    }


    @Override
    public void put(K key, V value, long expiresAt) throws BackendException {
        lock.writeLock().lock();
        try {
            String keyHash = keyToHash(key);
            byte[] keyBytes = serialize(key);
            byte[] valueBytes = serialize(value);
            long now = System.currentTimeMillis();

            putStmt.setString(1, keyHash);
            putStmt.setBytes(2, keyBytes);
            putStmt.setBytes(3, valueBytes);
            putStmt.setLong(4, expiresAt);
            putStmt.setLong(5, now);
            putStmt.executeUpdate();
        } catch (SQLException | IOException e) {
            throw new BackendException("Failed to put key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V get(K key) throws BackendException {
        lock.readLock().lock();
        try {
            String keyHash = keyToHash(key);
            getStmt.setString(1, keyHash);

            try (ResultSet rs = getStmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }

                long expiresAt = rs.getLong("expires_at");
                if (expiresAt != -1 && System.currentTimeMillis() > expiresAt) {
                    lock.readLock().unlock();
                    delete(key);
                    lock.readLock().lock();
                    return null;
                }

                byte[] valueBytes = rs.getBytes("value_bytes");
                return deserialize(valueBytes);
            }
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new BackendException("Failed to get key: " + key, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(K key) throws BackendException {

        lock.writeLock().lock();
        try {
            String keyHash = keyToHash(key);
            deleteStmt.setString(1, keyHash);
            deleteStmt.executeUpdate();
        } catch (SQLException e) {
            throw new BackendException("Failed to delete key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Map<K, V> loadAll() throws BackendException {

        lock.readLock().lock();
        try {
            Map<K, V> result = new HashMap<>();
            String query = "SELECT key_bytes, value_bytes, expires_at FROM livedict_entries";

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {

                long now = System.currentTimeMillis();
                while (rs.next()) {
                    long expiresAt = rs.getLong("expires_at");
                    if (expiresAt != -1 && now > expiresAt) {
                        continue;
                    }

                    byte[] keyBytes = rs.getBytes("key_bytes");
                    byte[] valueBytes = rs.getBytes("value_bytes");

                    K key = deserialize(keyBytes);
                    V value = deserialize(valueBytes);
                    result.put(key, value);
                }
            }
            return result;
        } catch (SQLException | IOException | ClassNotFoundException e) {
            throw new BackendException("Failed to load all entries", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void clear() throws BackendException {

        lock.writeLock().lock();
        try {
            clearStmt.executeUpdate();
        } catch (SQLException e) {
            throw new BackendException("Failed to clear database", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws BackendException {

        try {
            if (putStmt != null) putStmt.close();
            if (getStmt != null) getStmt.close();
            if (deleteStmt != null) deleteStmt.close();
            if (clearStmt != null) clearStmt.close();
            if (connection != null) connection.close();
            LOGGER.info("SqliteBackend closed: " + dbPath);
        } catch (SQLException e) {
            throw new BackendException("Failed to close SQLite backend", e);
        }
    }

    private String keyToHash(K key) {
        return key.toString();
    }

    private byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        }
    }

    private void startCleanupTask() {
        Thread cleanupThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(60_000);
                    cleanupExpired();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "sqlite-cleanup");
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    private void cleanupExpired() {
        lock.writeLock().lock();
        try {
            long now = System.currentTimeMillis();
            try (PreparedStatement stmt = connection.prepareStatement(
                    "DELETE FROM livedict_entries WHERE expires_at != -1 AND expires_at < ?")) {
                stmt.setLong(1, now);
                int deleted = stmt.executeUpdate();
                if (deleted > 0) {
                    LOGGER.fine("Cleaned up " + deleted + " expired entries from SQLite");
                }
            }
        } catch (SQLException e) {
            LOGGER.log(Level.WARNING, "Failed to cleanup expired entries", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
