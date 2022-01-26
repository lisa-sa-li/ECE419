package client;

import shared.messages.KVMessage;
import org.apache.log4j.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	String address;
	int port;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private static Logger logger = Logger.getRootLogger();


	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		try {
			Socket clientSocket = new Socket(address, port);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		//	logger.info("Connection established");

			logger.info("Connected to "
					+ clientSocket.getInetAddress().getHostName()
					+  " on port " + clientSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish connection. \n", e);
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		// setRunning(false);
		logger.info("Tearing down the connection ...");
		try {
			if (clientSocket != null) {
				clientSocket.close();
				clientSocket = null;
				logger.info("Connection closed!");
			}
		}  catch (IOException e) {
			logger.error("Error! Unable to close connection. \n", e);
		}

	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		// System.out.println(KVMessage.StatusType.PUT);
		// KVMessage req = new KVMessage(KVMessage.StatusType.PUT);
		// System.out.println(req);

		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
