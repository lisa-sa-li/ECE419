package app_kvServer;

import shared.messages.KVMessage.StatusType;

public interface IPersistantStorage {
	/**
	 * Puts a key-value pair into disk.
	 *
	 * @param key
	 *              the key that identifies the given value.
	 * @param value
	 *              the value that is indexed by the given key. If null, it is a
	 *              delete operation.
	 * @return true if key-value successfully put in file, else false.
	 * @throws Exception
	 *                   if put command cannot be executed (e.g. not connected to
	 *                   any
	 *                   KV server).
	 */
	public StatusType put(String key, String value) throws Exception;

	/**
	 * Retrieves the value for a given key from disk.
	 *
	 * @param key
	 *            the key that identifies the value.
	 * @return the value as a string, else null.
	 * @throws Exception
	 *                   if put command cannot be executed (e.g. not connected to
	 *                   any
	 *                   KV server).
	 */
	public String get(String key) throws Exception;

	public boolean inStorage(String key) throws Exception;

	public void deleteStorage();

	public void clearStorage();

}