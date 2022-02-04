package testing;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

public class InteractionTest extends TestCase {

	private KVStore kvStore;

	public void setUp() {
		kvStore = new KVStore("localhost", 50000);
		try {
			kvStore.connect();
		} catch (Exception e) {
			System.out.println("FAILED TO CONNECT CLIENT");
			System.out.println(e);

		}
	}

	public void tearDown() {
		kvStore.disconnect();
	}

	@Test
	public void testPut() {
		String key = "foo2", value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvStore.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutManyTerms() {
		String key = "foo3", value = "bar2 with spaces!";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvStore.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutLongKey() {
		String key = "tooooloooooooooooooong", value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvStore.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

	@Test
	public void testPutDisconnected() {
		kvStore.disconnect();
		String key = "foo", value = "bar";
		Exception ex = null;

		try {
			kvStore.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		JSONMessage response = null;
		Exception ex = null;

		try {
			kvStore.put(key, initialValue);
			response = kvStore.put(key, updatedValue);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}

	@Test
	public void testDelete() {
		String key = "deleteTestValue", value = "toDelete";

		JSONMessage response = null;
		Exception ex = null;

		try {
			kvStore.put(key, value);
			response = kvStore.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testGet() {
		String key = "foo", value = "bar";
		JSONMessage response = null;
		Exception ex = null;

		try {
			kvStore.put(key, value);
			response = kvStore.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvStore.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void testDeleteUnsavedValue() {
		String key = "tryDeletingValueDoesNotExist";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = kvStore.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	}

}
