package client;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.net.Socket;
import java.io.IOException;

import shared.exceptions.UnexpectedValueException;
import shared.messages.JSONMessage;
import shared.Utils;
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
import java.util.*;

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

	// Change for M2 --> Needs edit
	@Override
	public JSONMessage put(String key, String value) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value, null);
		return this.runPutGet(jsonMessage, key);
	}

	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, "", null);
		return this.runPutGet(jsonMessage, key);
	}

	public JSONMessage runPutGet(JSONMessage jsonMessage, String key) throws Exception {
		JSONMessage finalMsg = null;
		wasSuccessful = false;
		if ((this.currentNode == null) || (this.ECSNodeOrdered == null)) { // when initializing, first need to get
																			// metadata
			this.clientConnection.sendJSONMessage(jsonMessage);
			JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
			// Want to make sure it can successfully call function before stopping this
			// method
			int retries = 0;
			while (!wasSuccessful && retries < 3) {
				try {
					if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
						this.updateToCorrectNodeInfo(returnMsg, key);
						this.switchServer();
						this.clientConnection.sendJSONMessage(jsonMessage);
						returnMsg = this.clientConnection.receiveJSONMessage();
					} else {
						wasSuccessful = true;
						finalMsg = returnMsg;
					}
				} catch (Exception e) {
					logger.error(e);
				}
				retries++;
			}
		} else { // metadata already exists (might be stale)
			if (!(isECSNodeResponsibleForKey(key, this.currentNode))) {
				this.updateToCorrectNodeFromList(key);
				this.clientConnection.sendJSONMessage(jsonMessage);
				JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
				while (!wasSuccessful) {
					try {
						if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
							this.updateToCorrectNodeInfo(returnMsg, key);
							this.switchServer();
							this.clientConnection.sendJSONMessage(jsonMessage);
							returnMsg = this.clientConnection.receiveJSONMessage();
						} else {
							wasSuccessful = true;
							finalMsg = returnMsg;
						}
					} catch (Exception e) {
						logger.error(e);
					}
				}
			} else {
				this.clientConnection.sendJSONMessage(jsonMessage);
				JSONMessage returnMsg = this.clientConnection.receiveJSONMessage();
				while (!wasSuccessful) {
					try {
						if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
							this.updateToCorrectNodeInfo(returnMsg, key);
							this.switchServer();
							this.clientConnection.sendJSONMessage(jsonMessage);
							returnMsg = this.clientConnection.receiveJSONMessage();
						} else {
							wasSuccessful = true;
							finalMsg = returnMsg;
						}
					} catch (Exception e) {
						logger.error(e);
					}
				}
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
		// BigInteger keyHash = hashRing.getHash(key);
		BigInteger hash = currNode.getHash();
		BigInteger endHash = currNode.getEndHash();

		return utils.isKeyInRange(hash, endHash, key);

		// Three cases where the node is responsible for the key
		// Case 1: inHash <= key < endHash
		// Case 2: inHash >= endHash and key >= inHash and key >= endHash
		// Case 3: inHash >= endHash and key <= inHash and key <= endHash
		// return ((hash.compareTo(endHash) != 1) && (hash.compareTo(keyHash) != 1) &&
		// (endHash.compareTo(keyHash) != -1))
		// ||
		// ((hash.compareTo(endHash) != -1) && (hash.compareTo(keyHash) != 1) &&
		// (endHash.compareTo(keyHash) != 1))
		// ||
		// ((hash.compareTo(endHash) != -1) && (hash.compareTo(keyHash) != -1)
		// && (endHash.compareTo(keyHash) != -1));
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
				break;
			}
		}
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
				logger.error("Key from order is not in the format host:port");
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
