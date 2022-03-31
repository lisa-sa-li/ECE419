package testing;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.nio.file.*;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.FileNotFoundException;

import org.junit.Test;

import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

import app_kvECS.ECSClient;

public class ReplicaTest extends TestCase {

	private KVStore kvStore;
	private ECSClient ecs;
	public HashMap<String, String> serverInfo = new HashMap<String, String>();

	public void setUp() {
		ecs = new ECSClient("./test_servers.cfg");

		try {
			BufferedReader file = new BufferedReader(new FileReader("./test_servers.cfg"));
			StringBuffer inputBuffer = new StringBuffer();
			String line;
			String keyFromFile;

			while ((line = file.readLine()) != null) {
				// Get info from each line: name, host, port
				String[] info = line.split(" ");
				// int port = Integer.parseInt(serverInfo[2]);
				// Name, port:host
				serverInfo.put(info[1], info[0]);
			}
		} catch (Exception e) {

		}

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

	public String sendAndRecieve(KVStore inKVStore, String key) {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, null, null);

		JSONMessage response = null;

		try {
			inKVStore.clientConnection.sendJSONMessage(jsonMessage);
			response = inKVStore.clientConnection.receiveJSONMessage();
		} catch (Exception e) {
			System.out.println("Error sending or recieving message in ECS interaction test");
			return "";
		}

		return response.getValue();
	}

	public void clearFile(String pathToFile) {
		try {
			PrintWriter writer = new PrintWriter(pathToFile);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
		} catch (Exception e) {
		}
	}

	public void deleteFile(String pathToFile) {
		File file = new File(pathToFile);
		file.delete();
	}

	public long getFileLength(String pathToFile) {
		File file = new File(pathToFile);
		return file.length();
	}

	public String getAllFromFile(String pathToFile) {
		// Appends kv-pairs to the end of the storage file
		try {
			BufferedReader file = new BufferedReader(new FileReader(pathToFile));
			StringBuffer buffer = new StringBuffer();
			String line;

			while ((line = file.readLine()) != null) {
				buffer.append(line);
				buffer.append('\n');
			}

			file.close();
			return buffer.toString();
		} catch (Exception e) {
		}
		return "";
	}

	@Test
	public void testReplicasCreated() {
		Path path;
		File f;

		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				path = Paths.get(pathToFile);
				assertTrue(Files.exists(path));

				deleteFile(pathToFile);
			}
		}
	}

	@Test
	public void testPutGetsReplicated() {
		Path path;
		File f;

		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port = ports.get(0);
		String serverName = serverInfo.get(port);
		String host = "127.0.0.1";

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, "test", "1");
			// try {
			// TimeUnit.SECONDS.sleep(3);
			// } catch (Exception e) {

			// }
		} catch (Exception e) {
		}

		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nodeR.getNamePortHost() + ".txt";
			// path = Paths.get(pathToFile);
			assertTrue(getFileLength(pathToFile) == 1);
			assertTrue(getAllFromFile(pathToFile) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"1\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testPutUpdateGetsReplicated() {
		Path path;
		File f;

		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port = ports.get(0);
		String serverName = serverInfo.get(port);
		String host = "127.0.0.1";

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, "test", "1");
			sendAndRecieve(kvStore, "test", "2");
			// try {
			// TimeUnit.SECONDS.sleep(3);
			// } catch (Exception e) {

			// }
		} catch (Exception e) {
		}

		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nodeR.getNamePortHost() + ".txt";
			assertTrue(getFileLength(pathToFile) == 1);
			assertTrue(getAllFromFile(pathToFile) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"2\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testDeleteGetsReplicated() {
		Path path;
		File f;

		ecs.addNodes(3, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port = ports.get(0);
		String serverName = serverInfo.get(port);
		String host = "127.0.0.1";

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, "test", "1");
			sendAndRecieve(kvStore, "test", "");
			// try {
			// TimeUnit.SECONDS.sleep(3);
			// } catch (Exception e) {

			// }
		} catch (Exception e) {
		}

		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nodeR.getNamePortHost() + ".txt";
			assertTrue(getFileLength(pathToFile) == 0);
			// assertTrue(getAllFromFile(pathToFile) ==
			// "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"2\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testGetFromReplica() {
		Path path;
		File f;
		String value = "";

		ecs.addNodes(2, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port1 = ports.get(0);
		String serverName1 = serverInfo.get(port1);
		int port2 = ports.get(1);
		String serverName2 = serverInfo.get(port2);
		String host = "127.0.0.1";

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port1);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, host + port1, "1");
			kvStore.disconnect();
			// Switch to the other server, it should perform the GET on the replica of
			// server1
			kvStore = new KVStore(host, port2);
			kvStore.connect();
			value = sendAndRecieve(kvStore, host + port1);

			// try {
			// TimeUnit.SECONDS.sleep(3);
			// } catch (Exception e) {

			// }
		} catch (Exception e) {
		}

		assertTrue(value.equals("1"));

		kvStore.disconnect();
	}

	@Test
	public void testMoveReplicaData() {
		Path path;
		File f;
		String value = "";

		ecs.addNodes(2, "FIFO", 3);
		ecs.start();

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nodeR.getNamePortHost() + ".txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port1 = ports.get(0);
		String serverName1 = serverInfo.get(port1);
		int port2 = ports.get(1);
		String serverName2 = serverInfo.get(port2);
		String host = "127.0.0.1";

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port1);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, host + port1, "1");
			kvStore.disconnect();
			// Switch to the other server, it should perform the GET on the replica of
			// server1
			kvStore = new KVStore(host, port2);
			kvStore.connect();
			value = sendAndRecieve(kvStore, host + port1);

			// try {
			// TimeUnit.SECONDS.sleep(3);
			// } catch (Exception e) {

			// }
		} catch (Exception e) {
		}

		assertTrue(value.equals("1"));

		kvStore.disconnect();
	}

}
