package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.TextMessage;

import app_kvServer.KVServer;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ServerConnection implements IServerConnection, Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ServerConnection(Socket clientSocket, KVServer kvServer) throws Exception {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
		connect();
	}

	@Override
	public void connect() throws IOException {
		try {
			output = this.clientSocket.getOutputStream();
			input = this.clientSocket.getInputStream();

			logger.info("Connected to " + this.clientSocket.getInetAddress().getHostName() + " on port "
					+ this.clientSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n", e);
		}
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();
		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + ">: '"
				+ json.getJSON() + "'");
	}

	@Override
	public JSONMessage receiveJSONMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		int endChar = 0;
		while (reading && endChar < 3 && read != -1) {

			// Keep a count of EOMs to know when to stop reading
			if (read == 13 || read == 10) {
				endChar++;
			}

			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final Object */
		JSONMessage json = new JSONMessage();
		// bytes to string
		String jsonStr = json.byteToString(msgBytes);
		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			logger.debug("jsonStr is null in ServerConnection");
			return null;
		}
		// deserialize
		json.deserialize(jsonStr);
		logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort()
				+ ">: '" + json.getJSON().trim() + "'");
		return json;
	}

	private JSONMessage handleMessage(JSONMessage msg) throws IOException {
		String key = msg.getKey();
		String value = msg.getValue();
		StatusType status = msg.getStatus();

		String handleMessageValue = value.isEmpty() ? "" : value; // For PUT or DELETE, send the original value back
		StatusType handleMessageStatus = StatusType.NO_STATUS;
		JSONMessage handleMessage = new JSONMessage();

		switch (status) {
			case PUT:
				try {
					handleMessageStatus = this.kvServer.putKV(key, value);
					logger.info(handleMessageStatus.name() + ": key " + key + " & value " + value);
				} catch (Exception e) {
					handleMessageStatus = StatusType.PUT_ERROR;
					logger.info("PUT_ERROR: key " + key + " & value " + value);
				}
				break;
			case GET:
				try {
					handleMessageValue = this.kvServer.getKV(key);
					handleMessageStatus = StatusType.GET_SUCCESS;
					logger.info("GET_SUCCESS: key " + key + " & value " + handleMessageValue);
				} catch (Exception e) {
					handleMessageStatus = StatusType.GET_ERROR;
					logger.info("GET_ERROR: key " + key + " & value " + handleMessageValue);
				}
				break;
			default:
				logger.error("Unknown command.");
				break;
		}

		handleMessage.setMessage(handleMessageStatus.name(), key, handleMessageValue);
		return handleMessage;
	}

	public void run() {
		// while connection is open, listen for messages
		try {
			while (this.isOpen) {
				try {
					JSONMessage recievedMesage = receiveJSONMessage();
					if (recievedMesage != null) {
						JSONMessage sendMessage = handleMessage(recievedMesage);
						sendJSONMessage(sendMessage);
					}
				} catch (IOException e) {
					logger.error("Server connection lost: ", e);
					this.isOpen = false;
				} catch (Exception e) {
					logger.error(e);
				}
			}
		} finally {
			try {
				// close connection
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}
}
