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
				serverInfo.put(info[1], info[0]);
			}
		} catch (Exception e) {

		}

	}

	public void tearDown() {
		/*ecs.shutdown();
		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (Exception e) {

		}*/
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

	public JSONMessage sendAndRecieve(KVStore inKVStore, String key) {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, null, null);

		JSONMessage response = null;

		try {
			inKVStore.clientConnection.sendJSONMessage(jsonMessage);
			response = inKVStore.clientConnection.receiveJSONMessage();
		} catch (Exception e) {
			System.out.println("Error sending or recieving message in ECS interaction test");
			return null;
		}

		return response;
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

		ecs.addNodes(2, "FIFO", 3);
		ecs.start();
		// HashMap<String, Integer> replicatePorts = ecs.getReplicatePorts();

		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (Exception e) {
		}

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
				f = new File(pathToFile);
				assertTrue(f.exists());

				// deleteFile(pathToFile);
			}
		}
	}

	@Test
	public void testPutGetsReplicated() {
		System.out.println("testPutGetsReplicated");
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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
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

			String pathToFile = "./storage/repl_" + serverName + "_" +
					nodeR.getNamePortHost() + ".txt";
			// path = Paths.get(pathToFile);
			assertTrue(getFileLength(pathToFile) == 1);
			assertTrue(getAllFromFile(pathToFile) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"1\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testPutUpdateGetsReplicated() {
		System.out.println("testPutUpdateGetsReplicated");
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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
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

			String pathToFile = "./storage/repl_" + serverName + "_" +
					nodeR.getNamePortHost() + ".txt";
			assertTrue(getFileLength(pathToFile) == 1);
			assertTrue(getAllFromFile(pathToFile) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"2\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testDeleteGetsReplicated() {
		System.out.println("testDeleteGetsReplicated");
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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
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

			String pathToFile = "./storage/repl_" + serverName + "_" +
					nodeR.getNamePortHost() + ".txt";
			assertTrue(getFileLength(pathToFile) == 0);
			// assertTrue(getAllFromFile(pathToFile) ==
			// "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"2\"}\n");
		}

		kvStore.disconnect();
	}

	@Test
	public void testGetFromReplica() {
		System.out.println("testGetFromReplica");
		Path path;
		File f;
		JSONMessage value = null;

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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
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

		assertTrue(value.getValue().equals("1"));

		kvStore.disconnect();
	}

	@Test
	public void testMoveReplicaData() {
		System.out.println("testMoveReplicaData");
		Path path;
		File f;
		JSONMessage value = null;

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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
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

		assertTrue(value.getValue().equals("1"));

		kvStore.disconnect();
	}

	@Test
	public void testGetWhenNonExistent() {
		Path path;
		File f;

		ecs.addNodes(2, "None", 0);
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

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
				clearFile(pathToFile);
			}
		}

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		int port = ports.get(0);
		String serverName = serverInfo.get(port);
		String host = "127.0.0.1";

		// connect with KVStore and run one put and get
		KVStore kvStore = new KVStore(host, port);
		Exception ex_1 = null;
		JSONMessage returnString = null;
		try {
			kvStore.connect();
			returnString = sendAndRecieve(kvStore, "testing!", "yay");
			returnString = sendAndRecieve(kvStore, "testing!", "");
		} catch (Exception e) {
		}

		assertTrue(returnString == null); /////
		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
			assertTrue(getFileLength(pathToFile) == 1);
		}

		try {
			returnString = sendAndRecieve(kvStore, "testing!");
		} catch (Exception e) {
		}

		assertTrue(returnString == null);
		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";

			assertTrue(getFileLength(pathToFile) == 0);
		}
		kvStore.disconnect();
	}

	@Test
	public void testRemoveNodeWhatHappensToReplica() {
		Path path;
		File f;

		ecs.addNodes(2, "None", 0);
		ecs.start();

		// collect available servers
		ArrayList<Integer> ports = ecs.getCurrentPorts();
		System.out.println("ports: " + ports);
		int port = ports.get(0);
		String serverName = "";
		String host = "127.0.0.1";

		for (String name : ecs.currServerMap.keySet()) {
			ECSNode node = ecs.currServerMap.get(name);
			String namePortHost = node.getNamePortHost();
			String[] details = namePortHost.split(":");
			int portNumber = Integer.valueOf(details[1]);

			if (portNumber == port){
				serverName = details[0];
			}

			for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
				String nameR = entry.getKey();
				ECSNode nodeR = entry.getValue();

				if (nameR.equals(name)) {
					continue;
				}

				String pathToFile = "./storage/repl_" + name + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
						+ ":127.0.0.1_storage.txt";
				// f = new File(pathToFile);
				clearFile(pathToFile);
			}
		}

		System.out.println("serverName: " + serverName);

		// connect with KVStore
		KVStore kvStore = new KVStore(host, port);
		Exception ex_1 = null;
		try {
			kvStore.connect();
			sendAndRecieve(kvStore, "test", "1");
		} catch (Exception e) {
		}

		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverName)) {
				continue;
			}

			String pathToFile = "./storage/repl_" + serverName + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
					+ ":127.0.0.1_storage.txt";
			System.out.println("pathToFile: " + pathToFile);
			assertTrue(getFileLength(pathToFile) == 1);
			assertTrue(getAllFromFile(pathToFile) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"1\"}\n");
			System.out.println("getAllFromFile(pathToFile): " + getAllFromFile(pathToFile));
		}
		// Remove a node
		ArrayList<String> servers = ecs.getCurrentServers();
		ArrayList<String> nodeNames = new ArrayList<String>();
		nodeNames.add(servers.get(0));
		ecs.removeNodes(nodeNames);

		// connect to another client and check if the key is there
		ArrayList<Integer> portsAnother = ecs.getCurrentPorts();
		System.out.println("portsAnother: " + portsAnother);
		int port_another = 0;
		for (int i = 0; i < portsAnother.size(); i++){
			int potentialPort = portsAnother.get(i);
			if (potentialPort != port){
				port_another = potentialPort;
			}
		}
		System.out.println("port_another: " + port_another);
		String serverNameAnother = serverInfo.get(port);
		System.out.println("serverNameAnother: " + serverNameAnother);
		KVStore kvStore2 = new KVStore(host, port_another);

		JSONMessage output = null;
		try {
			kvStore2.connect();
			output = sendAndRecieve(kvStore2, "test");
		} catch (Exception e) {
		}

		kvStore2.disconnect();
		kvStore.disconnect();
		System.out.println("output: " + output.getValue());
		assertTrue(output.getValue().equals("1"));
		for (Map.Entry<String, ECSNode> entry : ecs.currServerMap.entrySet()) {
			String nameR = entry.getKey();
			ECSNode nodeR = entry.getValue();

			if (nameR.equals(serverNameAnother)) {
				continue;
			}

			String pathToFileAnother = "./storage/repl_" + serverNameAnother + "_" + nameR + ":" + nodeR.getReplicateReceiverPort()
					+ ":127.0.0.1_storage.txt";
			System.out.println("pathToFile: " + pathToFileAnother);
			// path = Paths.get(pathToFile);
			assertTrue(getFileLength(pathToFileAnother) == 1);
			assertTrue(getAllFromFile(pathToFileAnother) == "{\"status\":\"NO_STATUS\",\"key\":\"test\",\"value\":\"1\"}\n");
		}
	}
}
