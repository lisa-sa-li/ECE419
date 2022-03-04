package testing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

import app_kvECS.ECSClient;

public class ECSInteractionTest extends TestCase {

	private KVStore kvStore;
	private ECSClient ecs;

	public void setUp() {
		// kvStore = new KVStore("localhost", 50000);
		ecs = new ECSClient("./test_servers.cfg");
		// try {
		// kvStore.connect();
		// } catch (Exception e) {
		// System.out.println("FAILED TO CONNECT CLIENT");
		// System.out.println(e);

		// }
	}

	public void tearDown() {
		ecs.shutdown();
	}

	public JSONMessage sendAndRecieve(KVStore kvStore, String key, String value) {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value, null);
		// return this.runPutGet(jsonMessage, key);

		kvStore.clientConnection.sendJSONMessage(jsonMessage);
		return kvStore.clientConnection.receiveJSONMessage();
	}

	public JSONMessage put() throws Exception {

	}

	@Test
	public void singleServerRingTest() {
		// test adding a single server
		ecs.addNode("FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getAvailablePorts();

		assertTrue(ports.size() == 1);

		// connect with KVStore
		KVStore kvStore = new KVStore("127.0.0.1", ports.get(0));
		kvStore.connect();
	}

	@Test
	public void multipleServerRingTest() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getAvailablePorts();

		assertTrue(ports.size() == 3);

		// connect with KVStore
		KVStore kvStore_1 = new KVStore("127.0.0.1", ports.get(0));
		kvStore.connect();
		kvStore.disconnect();

		KVStore kvStore_2 = new KVStore("127.0.0.1", ports.get(1));
		kvStore.connect();
		kvStore.disconnect();

		KVStore kvStore_3 = new KVStore("127.0.0.1", ports.get(2));
		kvStore.connect();
		kvStore.disconnect();
	}

	@Test
	public void removeServerTest() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getAvailablePorts();
		ArrayList<String> servers = ecs.getAvailableServers();

		assertTrue(ports.size() == 3);

		// connect with KVStore
		KVStore kvStore_1 = new KVStore("127.0.0.1", ports.get(0));
		kvStore.connect();

		Collection<String> nodeNames;
		nodeNames.add(servers.get(0));

		ecs.removeNodes(nodeNames);

		ArrayList<String> ports_after = ecs.getAvailablePorts();
		assertTrue(ports.size() == 2);
	}

	@Test
	public void removeServersTest() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getAvailablePorts();
		ArrayList<String> servers = ecs.getAvailableServers();

		assertTrue(ports.size() == 3);

		// connect with KVStore
		KVStore kvStore_1 = new KVStore("127.0.0.1", ports.get(0));
		kvStore.connect();

		Collection<String> nodeNames;
		nodeNames.add(servers.get(0));
		nodeNames.add(servers.get(1));

		ecs.removeNodes(nodeNames);

		ArrayList<Integer> ports_after = ecs.getAvailablePorts();
		assertTrue(ports_after.size() == 1);
	}

	@Test
	public void testECSStart() {
		ecs.addnode("FIFO", 3);

		int port = ecs.getAvailablePorts().get(0);
		KVStore kvStore = new KVStore("127.0.0.1", port);
		kvStore.connect();
		ecs.start();

		String key = "foo2", value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() != StatusType.SERVER_STOPPED);
	}

	@Test
	public void testECSStop() {
		ecs.addnode("FIFO", 3);

		int port = ecs.getAvailablePorts().get(0);
		KVStore kvStore = new KVStore("127.0.0.1", port);

		ecs.stop();

		String key = "foo2", value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.SERVER_STOPPED);
	}

	@Test
	public void testServerNotResponsible() {
		ecs.addnodes(2, "FIFO", 3);

		int port = ecs.getAvailablePorts().get(0);
		KVStore kvStore = new KVStore("127.0.0.1", port);

		String key = "127.0.0.1:" + ecs.getAvailablePorts().get(1), value = "bar2";
		JSONMessage response = null;
		Exception ex = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
	}

	// @Test
	// public void testPutManyTerms() {
	// String key = "foo3", value = "bar2 with spaces!";
	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// response = kvStore.put(key, value);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	// }

	// @Test
	// public void testPutLongKey() {
	// String key = "tooooloooooooooooooong", value = "bar2";
	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// response = kvStore.put(key, value);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	// }

	// @Test
	// public void testPutDisconnected() {
	// kvStore.disconnect();
	// String key = "foo", value = "bar";
	// Exception ex = null;

	// try {
	// kvStore.put(key, value);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertNotNull(ex);
	// }

	// @Test
	// public void testUpdate() {
	// String key = "updateTestValue";
	// String initialValue = "initial";
	// String updatedValue = "updated";

	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// kvStore.put(key, initialValue);
	// response = kvStore.put(key, updatedValue);

	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
	// && response.getValue().equals(updatedValue));
	// }

	// @Test
	// public void testDelete() {
	// String key = "deleteTestValue", value = "toDelete";

	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// kvStore.put(key, value);
	// response = kvStore.put(key, "");
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	// }

	// @Test
	// public void testGet() {
	// String key = "foo", value = "bar";
	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// kvStore.put(key, value);
	// response = kvStore.get(key);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getValue().equals("bar"));
	// }

	// @Test
	// public void testGetUnsetValue() {
	// String key = "an unset value";
	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// response = kvStore.get(key);
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	// }

	// @Test
	// public void testDeleteUnsavedValue() {
	// String key = "tryDeletingValueDoesNotExist";
	// JSONMessage response = null;
	// Exception ex = null;

	// try {
	// response = kvStore.put(key, "");
	// } catch (Exception e) {
	// ex = e;
	// }

	// assertTrue(ex == null && response.getStatus() == StatusType.PUT_ERROR);
	// }

}
