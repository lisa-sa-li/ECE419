package client;

import org.apache.log4j.Logger;
import java.net.Socket;
import java.io.IOException;

import shared.exceptions.UnexpectedValueException;
import shared.messages.KVJsonMessage;
import shared.messages.KVMessage.StatusType;

import app_kvClient.ClientConnection;
import shared.messages.Metadata;
import java.util.List;

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
	private List<Metadata> metadata;

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
			clientConnection = new ClientConnection(clientSocket);

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
			KVJsonMessage jsonMessage = new KVJsonMessage();
			jsonMessage.setMessage(StatusType.DISCONNECTED.name(), "disconnected", "disconnected");
			this.clientConnection.sendJSONMessage(jsonMessage);
			this.clientConnection.receiveJSONMessage();
			this.clientConnection.close();
			logger.info("Client connection closed!");
		} catch (IOException e) {
			logger.error("Error! Unable to close connection. \n", e);
		}
	}

	@Override
	public KVJsonMessage put(String key, String value) throws Exception {
		KVJsonMessage jsonMessage = new KVJsonMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value);
		this.clientConnection.sendJSONMessage(jsonMessage);
		return this.clientConnection.receiveJSONMessage();
	}

	// for unit testing
	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, "");
		this.clientConnection.sendJSONMessage(jsonMessage);
		return this.clientConnection.receiveJSONMessage();
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

	public void switchServer(String address, int port) throws Exception {
		this.address = address;
		this.port = port;
		this.disconnect();
		try {
			this.connect();
		} catch (Exception e) {
			logger.error("The connection to the new server was not successful.");
		}
	}

	// Connects to the correct server and update the metadata if necessary
	public void connectToCorrectServer(KVJsonMessage msg, String key) throws Exception {
		this.metadata = msg.getMetadata();

	}

	// Sends message to the correct server (Used in put() and get())
	// IDK IF WE ARE RETURNING JSONMESSAGE
	public KVJsonMessage sendMessageToCorrectServer(KVJsonMessage msg, String key) throws Exception {
		this.clientConnection.sendJSONMessage(msg);
		KVJsonMessage returnMsg = this.clientConnection.receiveJSONMessage();
		if (returnMsg.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
			connectToCorrectServer(returnMsg, key);
			this.clientConnection.sendJSONMessage(msg);
			returnMsg = this.clientConnection.receiveJSONMessage();
		}
		return returnMsg;
	}

// 	Cache metadata of storage service. (Note: this metadata might not be the most recent)
// Route requests to the storage server that coordinates the respective key-range
// Metadata updating might be required which is initiated by the storage server if the client library, that caches the metadata, contacted a wrong storage server (i.e., the request could not be served by the storage server identified through the currently cached metadata) due to stale metadata
// Update metadata and retry the request

}
