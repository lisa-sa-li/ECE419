package client;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.net.Socket;
import java.io.IOException;

import shared.exceptions.UnexpectedValueException;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import ecs.HashRing;
import ecs.ECSNode;
import ecs.IECSNode.NodeStatus;

import app_kvClient.ClientConnection;
import shared.messages.Metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;

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
	private ClientConnection clientConnection;
	private Metadata metadata;
	private HashMap<String, BigInteger> metadataOrder;
	private HashRing hashRing;
	private ECSNode currReceiverNode;
	private List<Metadata> metadataList;
	private List<ECSNode> ECSNodeOrdered;
	private ECSNode currentNode;
	private boolean wasSuccessful;


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
			Socket clientSocket = new Socket(address, port);
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

	// Change for M2 --> Needs edit
	// Want to check the node from the existing metadata first before just randomly sending message (in the if statement before returning that function call)
	// Also, want to make sure it can successfully put before stopping this function --> maybe put it in while loop with boolean flag
	// this is to prep for the cases when server just dies randomly in the middle of putting
	// Actually, put while loop in sendMessageToCorrectServer() function might be better
	// Call updateToCorrectNodeInfo(JSONMessage msg, String key) in if statement before the return statement to check the stale metadata
	@Override
	public JSONMessage put(String key, String value) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value, null);
		if ((this.currentNode == null) || !(isECSNodeResponsibleForKey(key, this.currentNode))){
			return this.sendMessageToCorrectServer(jsonMessage, key);
		} else {
			this.clientConnection.sendJSONMessage(jsonMessage);
			return this.clientConnection.receiveJSONMessage();
		}
	}

	// Change for M2 --> Needs edit
	// Want to check the node from the previous metadata first before just randomly sending message
	// Also, want to make sure it succeeds before stopping this function
	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, "", null);
		if ((this.currentNode == null) || !(isECSNodeResponsibleForKey(key, this.currentNode))){
			return sendMessageToCorrectServer(jsonMessage, key);
		} else {
			this.clientConnection.sendJSONMessage(jsonMessage);
			return this.clientConnection.receiveJSONMessage();
		}
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
		BigInteger keyHash = hashRing.getHash(key);
		// Three cases where the node is responsible for the key
		// Case 1: inHash <= key <= endHash
		// Case 2: inHash >= endHash and key >= inHash and key >= endHash
		// Case 3: inHash >= endHash and key <= inHash and key <= endHash
		return ((currNode.getHash().compareTo(currNode.getEndHash()) != 1) && (currNode.getHash().compareTo(keyHash) != 1) && (currNode.getEndHash().compareTo(keyHash) != -1)) ||
				((currNode.getHash().compareTo(currNode.getEndHash()) != -1) && (currNode.getHash().compareTo(keyHash) != 1) && (currNode.getEndHash().compareTo(keyHash) != 1)) ||
				((currNode.getHash().compareTo(currNode.getEndHash()) != -1) && (currNode.getHash().compareTo(keyHash) != -1) && (currNode.getEndHash().compareTo(keyHash) != -1));
	}

	// Update the server address and port to have the most recent responsible node's information
	public void updateToCorrectNodeInfo(JSONMessage msg, String key) throws Exception {
		this.metadata = msg.getMetadata();
		this.metadataOrder = this.metadata.getOrder(); // String key 120.0.0.1:8008, BigInteger value inHash
		this.currReceiverNode = this.metadata.getReceiverNode();
		// Order the server nodes in ascending order in Array so that it can be iterated over to find the responsible server
		this.orderMetadataIntoECSNodeList(true);
		// Iterate over the Array of servers to find the responsible node and update the server address and server port
		for (int i = 0; i < this.ECSNodeOrdered.size(); i++) {
			boolean isNodeResponsibleForKey = isECSNodeResponsibleForKey(key, this.ECSNodeOrdered.get(i));
			if (isNodeResponsibleForKey) {
				this.currentNode = this.ECSNodeOrdered.get(i);
				this.address = this.currentNode.getNodeHost();
				this.port = this.currentNode.getNodePort();
			}
		}
	}

	public void orderMetadataIntoECSNodeList(final boolean ascending) {
		// From https://stackoverflow.com/questions/8119366/sorting-hashmap-by-values
		this.ECSNodeOrdered = new ArrayList<>();
		List<Entry<String, BigInteger>> metadataList = new LinkedList<Entry<String, BigInteger>>(this.metadataOrder.entrySet());
		Collections.sort(metadataList, new Comparator<Entry<String, BigInteger>>() {
			public int compare(Entry<String, BigInteger> o1, Entry<String, BigInteger> o2) {
				if (ascending) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());
				}
			}
		});
		for (Entry<String, BigInteger> entry : metadataList) {
			String[] keyList = entry.getKey().split(":");
			String serverAddress = keyList[0];
			int serverPort = Integer.valueOf(keyList[1]);
			ECSNode temp = new ECSNode(serverAddress, serverPort, entry.getValue());
			this.ECSNodeOrdered.add(temp);
		}
	}

	// Sends message to the correct server (Used in put() and get()) --> Needs edit
	public JSONMessage sendMessageToCorrectServer(JSONMessage msg, String key) throws Exception {
		this.clientConnection.sendJSONMessage(msg);
		JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
		if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			this.updateToCorrectNodeInfo(returnMsg, key);
			this.switchServer();
			this.clientConnection.sendJSONMessage(msg);
			returnMsg = this.clientConnection.receiveJSONMessage();
		}
		return returnMsg;
	}

	// Cache metadata of storage service. (Note: this metadata might not be the most recent)
	// Route requests to the storage server that coordinates the respective key-range
	// Metadata updating might be required which is initiated by the storage server if the client library, that caches the metadata, contacted a wrong storage server (i.e., the request could not be served by the storage server identified through the currently cached metadata) due to stale metadata
	// Update metadata and retry the request

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
