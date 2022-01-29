package client;

import org.apache.log4j.Logger;
import java.net.Socket;
import java.io.IOException;

import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

import app_kvClient.ClientConnection;

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
	private Socket clientSocket;
	private static Logger logger = Logger.getRootLogger();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private ClientConnection clientConnection;

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.clientID = -1;
	}

	// class for testing
	public KVStore(String address, int port, int maxUsers, int clientID) {
		this.address = address;
		this.port = port;
		this.maxUsers = maxUsers;
		this.clientID = clientID;
	}

	@Override
	public void connect() throws Exception {
		try {
			System.out.println("IN CONNECT: "+ address + port);
			Socket clientSocket = new Socket(address, port);
			System.out.println("IN after socket");
			clientConnection = new ClientConnection(clientSocket);
			System.out.println("IN after client connects");

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
	public JSONMessage put(String key, String value) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.PUT.name(), key, value);
		this.clientConnection.sendJSONMessage(jsonMessage);
		return this.clientConnection.receiveJSONMessage();
	}

	// for testing
	public void run() {
		int totalUsers = clientID + maxUsers;
		try {
			// connect to socket
			connect();

			System.out.println("umpteenth: ");


			// put request per thread
			for (int i = clientID; i < totalUsers; i ++){
				System.out.println("loop 1: ");
				put("cake" + i, "icing" + i);
				System.out.println("loop 11: ");
			}

			// update previous entry per thread (test persistent storage)
			for (int i = clientID; i < totalUsers; i ++){
				System.out.println("loop 2: ");
				put("cake" + (i-1), "icing" + (i-1));
				System.out.println("loop 22: ");

			}

			// get entry per thread
			String value;
			for (int i = clientID; i < totalUsers; i ++){
				System.out.println("loop 3: ");
				value = get("cake" + i).getValue();
				System.out.println("GET TEST THREAD VALUE: "+value);

				// if (!value.equals("icing" + (i-1))){
				// 	testSuccess = false;
				// }
			} 
			disconnect();
		}
		catch (Exception e){
			System.out.println("ERROR: " + e);
		}
	}

	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage(StatusType.GET.name(), key, "");
		this.clientConnection.sendJSONMessage(jsonMessage);
		return this.clientConnection.receiveJSONMessage();
	}
}
