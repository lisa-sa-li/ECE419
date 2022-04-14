package testing;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

import app_kvECS.ECSClient;

public class ECSInteractionTest extends TestCase {

	private KVStore kvStore;
	private ECSClient ecs;

	public void setUp() {
		ecs = new ECSClient("./test_servers.cfg");
	}

	public void tearDown() {
		ecs.shutdown();
		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (Exception e) {

		}
	}

	public JSONMessage sendAndRecieve(KVStore inKVStore, String key, String value) {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value, null);

		JSONMessage response = null;

		try {
			inKVStore.clientConnection.sendJSONMessage(jsonMessage);
			response = inKVStore.clientConnection.receiveJSONMessage();
		} catch (Exception e) {
			System.out.println("Error sending or recieving message in ECS interaction test");
		}

		return response;
	}

	@Test
	public void testSingleServerRing() {
		// test adding a single server
		ecs.addNode("FIFO", 3);
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (Exception e) {
		}
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();

		assertTrue(ports.size() == 1);

		// connect with KVStore
		KVStore kvStore_test = new KVStore("127.0.0.1", ports.get(0));

		Exception ex = null;
		try {
			kvStore_test.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null);
		kvStore_test.disconnect();
	}

	@Test
	public void testMultipleServerRingTest() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();

		assertTrue(ports.size() == 3);

		// connect with KVStore
		KVStore kvStore_1 = new KVStore("127.0.0.1", ports.get(0));
		Exception ex_1 = null;
		try {
			kvStore_1.connect();
		} catch (Exception e) {
			ex_1 = e;
		}

		assertTrue(ex_1 == null);

		kvStore_1.disconnect();

		KVStore kvStore_2 = new KVStore("127.0.0.1", ports.get(1));
		Exception ex_2 = null;
		try {
			kvStore_2.connect();
		} catch (Exception e) {
			ex_2 = e;
		}

		assertTrue(ex_2 == null);

		kvStore_2.disconnect();

		KVStore kvStore_3 = new KVStore("127.0.0.1", ports.get(2));
		Exception ex_3 = null;
		try {
			kvStore_3.connect();
		} catch (Exception e) {
			ex_3 = e;
		}

		assertTrue(ex_3 == null);

		kvStore_3.disconnect();
	}

	@Test
	public void testRemoveServer() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		ArrayList<String> servers = ecs.getCurrentServers();

		assertTrue(ports.size() == 3 && servers.size() == 3);

		ArrayList<String> nodeNames = new ArrayList<String>();
		nodeNames.add(servers.get(0));

		ecs.removeNodes(nodeNames);

		ArrayList<Integer> ports_after = ecs.getCurrentPorts();
		assertTrue(ports_after.size() == 2);
	}

	@Test
	public void testRemoveServers() {
		// test adding multiples servers
		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		ArrayList<String> servers = ecs.getCurrentServers();

		assertTrue(ports.size() == 3 && servers.size() == 3);

		ArrayList<String> nodeNames = new ArrayList<String>();
		nodeNames.add(servers.get(0));
		nodeNames.add(servers.get(1));

		ecs.removeNodes(nodeNames);

		ArrayList<Integer> ports_after = ecs.getCurrentPorts();
		assertTrue(ports_after.size() == 1);
	}

	@Test
	public void testECSStart() {
		ecs.addNode("FIFO", 3);

		int port = ecs.getCurrentPorts().get(0);
		kvStore = new KVStore("127.0.0.1", port);
		Exception ex = null;
		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);

		ecs.start();

		String key = "foo2", value = "bar2";
		JSONMessage response = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() != StatusType.SERVER_STOPPED);
		kvStore.disconnect();
	}

	@Test
	public void testECSStop() {
		ecs.addNode("FIFO", 3);
		try {
			TimeUnit.SECONDS.sleep(3);
		} catch (Exception e) {
		}
		ecs.stop();

		int port = ecs.getCurrentPorts().get(0);
		KVStore kvStore = new KVStore("localhost", port);
		Exception ex = null;
		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null);

		String key = "foo2", value = "bar2";
		JSONMessage response = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.SERVER_STOPPED);
		kvStore.disconnect();
	}

	@Test
	public void testServerNotResponsible() {
		ecs.addNodes(2, "FIFO", 3);
		try {
			TimeUnit.SECONDS.sleep(10);
			ecs.start();
			TimeUnit.SECONDS.sleep(5);
		} catch (Exception e) {
		}

		int port = ecs.getCurrentPorts().get(0);
		KVStore kvStore = new KVStore("127.0.0.1", port);
		Exception ex = null;
		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null);

		String key = "127.0.0.1:" + ecs.getCurrentPorts().get(1), value = "bar2";
		JSONMessage response = null;

		try {
			response = sendAndRecieve(kvStore, key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
		kvStore.disconnect();
	}

}
