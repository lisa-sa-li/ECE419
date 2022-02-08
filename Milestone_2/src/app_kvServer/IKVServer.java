package app_kvServer;

import shared.messages.KVMessage.StatusType;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    public enum ServerStatus {
        OPEN,
        CLOSED,
        SHUTDOWN,
        LOCKED,
    };

    /**
     * Get the port number of the server
     * 
     * @return port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * 
     * @return hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * 
     * @return cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * 
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inStorage(String key) throws Exception;

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * 
     * @return true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * 
     * @return value associated with key
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * 
     * @throws Exception
     *                   when key not in the key range of the server
     */
    public StatusType putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Delete the storage of the server
     */
    public void deleteStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    // Starts the KVServer, all client requests and all ECS requests are processed.
    public void start();

    // Stops the KVServer, all client requests are rejected and only ECS requests
    // are processed.
    public void stop();

    // Exits the KVServer application.
    public void shutDown();

    // Lock the KVServer for write operations.
    public void lockWrite();

    // Unlock the KVServer for write operations.
    public void unLockWrite();
}
