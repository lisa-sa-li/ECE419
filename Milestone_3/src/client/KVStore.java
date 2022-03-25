package client;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.net.Socket;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.*;

import ecs.HashRing;
import ecs.ECSNode;
import ecs.IECSNode.NodeStatus;

import app_kvClient.ClientConnection;
import app_kvServer.Controller;

import shared.messages.Metadata;
import shared.messages.KVMessage.StatusType;
import shared.messages.JSONMessage;
import shared.exceptions.UnexpectedValueException;
import shared.Utils;

public class KVStore implements KVCommInterface, Runnable {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	String address;
	int port;
	private int clientID;
	private int maxUsers;
	private boolean running;
	private Socket clientSocket;
	private static Logger logger = Logger.getRootLogger();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	public ClientConnection clientConnection;
	private Metadata metadata;
	private HashMap<String, BigInteger> metadataOrder;
	private HashRing hashRing;
	private ECSNode currReceiverNode;
	private List<Metadata> metadataList;
	private List<ECSNode> ECSNodeOrdered;
	private ECSNode currentNode;
	private boolean wasSuccessful;
	private List<ECSNode> currReplicants;
	private String namePortHost;
	private String nodeName;
	private HashMap<String, List<ECSNode>> nodePortHostVSListECSNode;
	private HashMap<String, List<String>> nodePortHostVSListECSNodeKeyIsAReplicaOf;

	private Utils utils = new Utils();

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.clientID = -1;
	}

	// initialization for unit testing
	public KVStore(String address, int port, int clientID, int maxUsers) {
		this.address = address;
		this.port = port;
		this.maxUsers = maxUsers;
		this.clientID = clientID;
	}

	@Override
	public void connect() throws Exception {
		try {
			Socket clientSocket = new Socket(this.address, this.port);
			this.clientConnection = new ClientConnection(clientSocket);
			logger.info("Connected to " + clientSocket.getInetAddress().getHostName() + " on port "
					+ clientSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish connection to store. \n", e);
			throw e;
		}
	}

	@Override
	public void disconnect() {
		logger.info("Tearing down the connection ...");
		try {
			JSONMessage jsonMessage = new JSONMessage();
			jsonMessage.setMessage(StatusType.DISCONNECTED.name(), "disconnected", "disconnected", null);

			this.clientConnection.sendJSONMessage(jsonMessage);
			this.clientConnection.receiveJSONMessage();
			this.clientConnection.close();
			logger.info("Client connection closed!");
		} catch (IOException e) {
			logger.error("Error! Unable to close connection. \n", e);
		}
	}

	@Override
	public JSONMessage put(String key, String value) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value, null);
		return this.runPutGet(jsonMessage, key, false);
	}

	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, "", null);
		return this.runPutGet(jsonMessage, key, true);
	}

	public JSONMessage runPutGet(JSONMessage jsonMessage, String key, boolean getBoolean) throws Exception {
		JSONMessage finalMsg = null;
		wasSuccessful = false;
		// System.out.println("Inside runPutGet in kvstore");
		if (this.currentNode != null && this.ECSNodeOrdered != null
				&& !isECSNodeResponsibleForKey(key, this.currentNode) ) {
			if ((getBoolean && this.nodePortHostVSListECSNodeKeyIsAReplicaOf == null) || (!getBoolean)){
				//System.out.println("Metadata stale: original address: " + this.address + " original port: " + this.port);
				// metadata already exists (might be stale)
				this.updateToCorrectNodeFromList(key);
				//System.out.println("Updated to correct node info: " + this.address + " new port: " + this.port);
			}
		}

		if (getBoolean && this.nodePortHostVSListECSNodeKeyIsAReplicaOf != null) {
			// Check the replicas if one of them are responsible for key
			String tempNamePortHost = this.currentNode.getNodeName() + ":"
					+ this.currentNode.getNodePort() + ":" + this.currentNode.getNodeHost();
			List<String> tempNodeIsAReplicaOf = this.nodePortHostVSListECSNodeKeyIsAReplicaOf.get(tempNamePortHost);
			for (int i = 0; i < tempNodeIsAReplicaOf.size(); i++) {
				// ECSNode tempNodeAnother = tempNodeIsAReplicaOf.get(i);
				// String tempNamePortHostAnother = tempNodeAnother.getNodeName() + ":"
				// 		+ tempNodeAnother.getNodePort() + ":" + tempNodeAnother.getNodeHost();
				// this.currentNode = tempNodeAnother;
				String tempNamePortHostAnother = tempNodeIsAReplicaOf.get(i);
				String[] tempNamePortHostAnotherSeparate = tempNamePortHostAnother.split(":");
				this.address = tempNamePortHostAnotherSeparate[2];
				this.port = Integer.valueOf(tempNamePortHostAnotherSeparate[1]);
				this.nodeName = tempNamePortHostAnotherSeparate[0];
				this.namePortHost = tempNamePortHostAnother;
				this.currentNode = new ECSNode(this.nodeName, this.port, this.address);
				this.switchServer();
				this.clientConnection.sendJSONMessage(jsonMessage);
				JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
				if (returnMsg.getStatus() == StatusType.GET_SUCCESS || returnMsg.getStatus() == StatusType.GET_ERROR
						|| returnMsg.getStatus() == StatusType.GET) {
					finalMsg = returnMsg;
					break;
				}
			}
		}

		if (finalMsg == null) {
			this.updateToCorrectNodeFromList(key);
			this.clientConnection.sendJSONMessage(jsonMessage);
			JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
			// System.out.println("sent message before while loop: " + returnMsg.getStatus().toString());
			int retries = 0;
			while (!wasSuccessful && retries < 3) {
				try {
					if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
						//System.out.println("Server not responsible so switching soon");
						this.updateToCorrectNodeInfo(returnMsg, key);
						this.switchServer();
						// System.out.println("switched to: " + this.address + " address and port: " + this.port);
						this.clientConnection.sendJSONMessage(jsonMessage);
						// System.out.println("Sent message");
						returnMsg = this.clientConnection.receiveJSONMessage();
						// System.out.println("returned message: " + returnMsg.getStatus().toString());
					} else {
						wasSuccessful = true;
						finalMsg = returnMsg;
						// System.out.println("server was responsible: returned message: " + returnMsg.getStatus().toString());
					}
				} catch (Exception e) {
					logger.error(e);
				}
				retries++;
			}
		}
		return finalMsg;
	}

	public void switchServer() throws Exception {
		this.disconnect();
		try {
			this.connect();
		} catch (Exception e) {
			logger.error("The connection to the new server was not successful.");
		}
	}

	// Find out if the current ECSNode is responsible for the given key
	public boolean isECSNodeResponsibleForKey(String key, ECSNode currNode) {
		BigInteger hash = currNode.getHash();
		BigInteger endHash = currNode.getEndHash();
		return utils.isKeyInRange(hash, endHash, key);
	}

	// Update the server address and port to have the most recent responsible node's
	// information
	public void updateToCorrectNodeInfo(JSONMessage msg, String key) throws Exception {
		this.metadata = msg.getMetadata();
		this.metadataOrder = this.metadata.getOrder(); // String key 120.0.0.1:8008, BigInteger value inHash
		this.currReceiverNode = this.metadata.getReceiverNode();
		// Order the server nodes in ascending order in Array so that it can be iterated
		// over to find the responsible server
		this.orderMetadataIntoECSNodeList(true);
		this.updateToCorrectNodeFromList(key);
	}

	// Iterate over the Array of servers to find the responsible node and update the
	// server address and server port
	public void updateToCorrectNodeFromList(String key) {
		for (int i = 0; i < this.ECSNodeOrdered.size(); i++) {
			boolean isNodeResponsibleForKey = isECSNodeResponsibleForKey(key, this.ECSNodeOrdered.get(i));
			if (isNodeResponsibleForKey) {
				this.currentNode = this.ECSNodeOrdered.get(i);
				this.address = this.currentNode.getNodeHost();
				this.port = this.currentNode.getNodePort();
				this.nodeName = this.currentNode.getNodeName();
				this.namePortHost = this.nodeName + ":" + this.port + ":" + this.address;
				this.currReplicants = this.setReplicationServers(this.metadataOrder, this.namePortHost);
				this.nodePortHostVSListECSNode.put(this.namePortHost, this.currReplicants);
				break;
			} else {
				ECSNode tempNode = this.ECSNodeOrdered.get(i);
				String tempNamePortHost = tempNode.getNodeName() + ":" + tempNode.getNodePort() + ":" + tempNode.getNodeHost();
				List<ECSNode> currECSNodeTempListReplica = this.setReplicationServers(this.metadataOrder, tempNamePortHost);
				this.nodePortHostVSListECSNode.put(tempNamePortHost, currECSNodeTempListReplica);
			}
		}
		this.findNodeIsAReplicaOf(this.nodePortHostVSListECSNode);
	}

	public void orderMetadataIntoECSNodeList(final boolean ascending) {
		// From https://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
		this.ECSNodeOrdered = new ArrayList<>();
		List<Entry<String, BigInteger>> metadataList = new LinkedList<Entry<String, BigInteger>>(
				this.metadataOrder.entrySet());
		Collections.sort(metadataList, new Comparator<Entry<String, BigInteger>>() {
			public int compare(Entry<String, BigInteger> o1, Entry<String, BigInteger> o2) {
				if (ascending) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());
				}
			}
		});

		// populate the end hash
		for (int i = 0; i < metadataList.size(); i++) {
			Entry<String, BigInteger> entry = metadataList.get(i);

			String[] keyList = entry.getKey().split(":");
			if (keyList.length != 3) {
				logger.error("Key from order is not in the format name:port:host");
				continue;
			}

			String serverName = keyList[0];
			int serverPort = Integer.valueOf(keyList[1]);
			String serverHost = keyList[2];

			BigInteger endHash;
			if (i == metadataList.size() - 1) {
				endHash = metadataList.get(0).getValue();
			} else {
				endHash = metadataList.get(i + 1).getValue();
			}
			this.ECSNodeOrdered.add(new ECSNode(serverName, serverHost, serverPort, entry.getValue(), endHash));
		}
	}

	// From Controller.java
	public String getServerByHash(HashMap<String, BigInteger> map, BigInteger value) {
		for (Entry<String, BigInteger> entry : map.entrySet()) {
			if (Objects.equals(value, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	// From Controller.java
	public List<ECSNode> setReplicationServers(HashMap<String, BigInteger> hashRing, String namePortHostTemp) {
		Collection<BigInteger> keys = hashRing.values();
		ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
		Collections.sort(orderedKeys);
		BigInteger currHash = hashRing.get(namePortHostTemp);
		List<ECSNode> tempListECSNode = new ArrayList<ECSNode>();

		// If it's the only server in the hashring, no replicates
		if (orderedKeys.size() == 1) {
			logger.info("No replicants possible: only 1 node in hashring");
			return null;
		}

		// find first replicant
		Integer firstIdx = orderedKeys.indexOf(currHash);
		firstIdx = (firstIdx + 1) % orderedKeys.size();
		String namePortHost = getServerByHash(hashRing, orderedKeys.get(firstIdx));
		String[] replicant1Info = namePortHost.split(":");
		ECSNode firstReplicant = new ECSNode(replicant1Info[0], replicant1Info[1], replicant1Info[2]);
		tempListECSNode.add(firstReplicant);

		// check if second replicant possible
		if (orderedKeys.size() == 2) {
			logger.info("Only 1 replicant possible: 2 nodes total in the ring");
			return tempListECSNode;
		}

		Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
		namePortHost = getServerByHash(hashRing, orderedKeys.get(secondIdx));
		String[] replicant2Info = namePortHost.split(":");
		ECSNode secondReplicant = new ECSNode(replicant2Info[0], replicant2Info[1], replicant2Info[2]);
		tempListECSNode.add(secondReplicant);

		return tempListECSNode;
	}

	public void findNodeIsAReplicaOf(HashMap<String, List<ECSNode>> mapNamePortNostVSListECSNode) {
		ArrayList<String> keys = new ArrayList<>(mapNamePortNostVSListECSNode.keySet());
		for (int i = 0; i < keys.size(); i++) {
			List<ECSNode> tempList = mapNamePortNostVSListECSNode.get(keys.get(i));
			if (tempList != null) {
				for (int j = 0; j < tempList.size(); j++) {
					this.nodePortHostVSListECSNodeKeyIsAReplicaOf.get(tempList.get(j)).add(keys.get(i));
				}
			}
		}
	}

	// for testing
	public void run() {
		running = true;
		int totalUsers = clientID + maxUsers;
		while (running) {
			try {
				// connect to socket
				connect();
				// put request
				put("cake" + clientID, "icing" + clientID);
				// get entry
				String value;
				value = get("cake" + clientID).getValue();
				if (!value.equals("icing" + clientID)) {
					throw new UnexpectedValueException("Unexpected read value: " + value + " for client: " + clientID);
				} else {
					System.out.println("SUCCESS: read value: " + value + " for client: " + clientID);
				}
				// disconnect from server
				disconnect();
				running = false;
			} catch (Exception e) {
				System.out.println("ERROR: " + e);
			}
		}
	}

}
