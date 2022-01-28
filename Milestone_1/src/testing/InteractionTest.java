package testing;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

public class InteractionTest extends TestCase {

	private KVStore kvClient;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			System.out.println("FAILED TO CONNECT CLIENT");
			System.out.println(e);

		}
	}

	public void tearDown() {
		System.out.println("DISCONNECT CLIENT");
		kvClient.disconnect();
		// System.out.println("CLEAR CACHE");
		// kvServer.clearCache();
		// System.out.println("CLEAR STORAGE");
		// kvServer.clearStorage();
		// System.out.println("CLOSE");
		// kvServer.close();
		// System.out.println("DONE");
	}

	@Test
	public void testPut() {
		System.out.println("TEST PUT");

		String key = "foo2";
		String value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	// @Test
	// public void testPutDisconnected() {
	// 	kvClient.disconnect();
	// 	String key = "foo";
	// 	String value = "bar";
	// 	Exception ex = null;

	// 	try {
	// 		kvClient.put(key, value);
	// 	} catch (Exception e) {
	// 		ex = e;
	// 	}

	// 	assertNotNull(ex);
	// }

	@Test
	public void testUpdate() {
		System.out.println("TEST UPDATE");

		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		System.out.println("TEST DELETE");

		String key = "deleteTestValue";
		String value = "toDelete";

		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}
		System.out.println("TEST DELETE STATUS: " + response.getStatus());

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		System.out.println("TEST GET");

		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		System.out.println("TEST GET UNSET VALUE");

		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

}
