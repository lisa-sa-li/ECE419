package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.math.BigInteger;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import logging.ServerLogSetup;
import ecs.ECSNode;

import shared.exceptions.KeyValueTooLongException;
import shared.exceptions.UnexpectedValueException;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import shared.messages.Metadata.MessageType;
import shared.messages.Metadata;

import app_kvServer.KVServer;
import app_kvServer.IKVServer.ServerStatus;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 */
public class ServerConnection implements IServerConnection, Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket serverSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	/**
	 * Constructs a new ServerConnection object for a given TCP socket.
	 * 
	 * @param serverSocket the Socket object for the server connection.
	 */
	public ServerConnection(Socket serverSocket, KVServer kvServer) throws Exception {
		new ServerLogSetup("logs/serverConnection.log", Level.ALL);

		this.serverSocket = serverSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
		connect();
	}

	@Override
	public void connect() throws IOException {
		try {
			output = this.serverSocket.getOutputStream();
			input = this.serverSocket.getInputStream();

			logger.info("Connected to " + this.serverSocket.getInetAddress().getHostName() + " on port "
					+ this.serverSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n" + e);
		}
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();
		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort() + ">: '"
				+ json.getJSON() + "'");
	}

	@Override
	public JSONMessage receiveJSONMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		byte read = (byte) input.read();
		boolean reading = true;

		// Check if stream is closed (read returns -1)
		if (read == -1) {
			JSONMessage json = new JSONMessage();
			json.setMessage(StatusType.DISCONNECTED.name(), "disconnected", "disconnected");
			return json;
		}

		int endChar = 0;
		while (reading && endChar < 3 && read != -1) {
			// Keep a count of EOMs to know when to stop reading
			// 13 = CR, 10 = LF/NL
			if (read == 13 || read == 10) {
				endChar++;
			}

			// If buffer filled, copy to msg array
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			// Only read valid characters, i.e. letters and constants
			bufferBytes[index] = read;
			index++;

			// Stop reading is DROP_SIZE is reached
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			// Read next char from stream
			read = (byte) input.read();
			// logger.debug("recieve10");
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

		// Build final Object and convert from bytes to string
		JSONMessage json = new JSONMessage();
		String jsonStr = json.byteToString(msgBytes);

		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			return null;
		}

		json.deserialize(jsonStr);
		logger.info("RECEIVE \t<" + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort()
				+ ">: '" + json.getJSON().trim() + "'");
		return json;
	}

	private JSONMessage handleMessage(JSONMessage msg) throws IOException {
		String key = msg.getKey();
		String value = msg.getValue();
		StatusType status = msg.getStatus();

		String handleMessageValue = value; // For PUT or DELETE, send the original value back
		StatusType handleMessageStatus = StatusType.NO_STATUS;
		JSONMessage handleMessage = new JSONMessage();

		HashMap<String, BigInteger> order = null;

		switch (status) {
			case PUT:
				try {
					if (!this.kvServer.isMe(key)) {
						handleMessageStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						// send back metadata
						order = this.kvServer.getOrder();
						break;
					}
					// check key, value length
					/*
					if (key.length() > 20) {
						throw new KeyValueTooLongException("Key too long: " + key);
					}*/
					if (key.trim().isEmpty() || key == null) {
						throw new InvalidKeyException("Invalid key: " + key);
					}
					/*if (value.length() > 120000) {
						throw new KeyValueTooLongException("Value too long : " + value);
					}*/
					handleMessageStatus = this.kvServer.putKV(key, value);
					logger.info(handleMessageStatus.name() + ": key " + key + " & value " + value);
				} catch (Exception e) {
					handleMessageStatus = StatusType.PUT_ERROR;
					logger.info("PUT_ERROR: key " + key + " & value " + value);
				}
				break;
			case PUT_MANY:
				// When another KVServer passes this server data due to the hashring changing
				try {
					handleMessageStatus = this.kvServer.appendToStorage(value);
					handleMessageStatus = null;
					logger.info("PUT_MANY success");
				} catch (Exception e) {
					handleMessageStatus = StatusType.PUT_ERROR;
					logger.info("PUT_ERROR: key " + key + " & value " + value);
				}
				break;
			case GET:
				if (!this.kvServer.isMe(key)) {
					handleMessageStatus = StatusType.SERVER_NOT_RESPONSIBLE;
					order = this.kvServer.getOrder();
					// send back metadata
					break;
				}
				try {
					/* if (key.length() > 20) {
						throw new KeyValueTooLongException("Key too long: " + key);
					}*/
					if (!value.trim().isEmpty() && value != null) {
						throw new UnexpectedValueException("Unexpected value for GET: " + value);
					}
					if (key.trim().isEmpty() || key == null) {
						throw new InvalidKeyException("Invalid key: " + key);
					}
					handleMessageValue = this.kvServer.getKV(key);
					handleMessageStatus = StatusType.GET_SUCCESS;
					logger.info("GET_SUCCESS: key " + key + " & value " + handleMessageValue);
				} catch (Exception e) {
					handleMessageStatus = StatusType.GET_ERROR;
					logger.info("GET_ERROR: key " + key + " & value " + handleMessageValue);
				}
				break;
			case DISCONNECTED:
				this.isOpen = false;
				handleMessageStatus = StatusType.DISCONNECTED;
				logger.info("Client is disconnected");
				break;
			default:
				logger.error("Unknown command.");
				break;
		}

		if (handleMessageStatus == null) {
			// This is a PUT_MANY success, do not send a message back to the sender node
			return null;
		} else if (handleMessageStatus == StatusType.SERVER_NOT_RESPONSIBLE) {
			Metadata metadata = new Metadata(MessageType.SERVER_NOT_RESPONSIBLE, order, null);
			handleMessage.setMessage(handleMessageStatus.name(), key, handleMessageValue, metadata);
		} else {
			handleMessage.setMessage(handleMessageStatus.name(), key, handleMessageValue);
		}

		return handleMessage;
	}

	private JSONMessage handleMetadataMessage(Metadata message) {
		// String handleMessageValue = value; // For PUT or DELETE, send the original
		// value back
		StatusType handleMessageStatus = StatusType.NO_STATUS;
		JSONMessage handleMessage = new JSONMessage();
		String key = "";
		String value = "";

		try {
			switch (message.getStatus()) {
				case START:
					this.kvServer.start();
					break;
				case STOP:
					this.kvServer.stop();
					break;
				case SHUTDOWN:
					this.kvServer.shutDown();
					break;
				case LOCKED:
					this.kvServer.lockWrite();
					break;
				case UNLOCK:
					this.kvServer.unLockWrite();
					break;
				case SET_METADATA:
					if (this.kvServer.getOrder() == null) {
						this.kvServer.initKVServer(message);
					} else {
						this.kvServer.update(message);
					}
					key = "receieved";
					value = "message";
					break;
				case MOVE_DATA:
					this.kvServer.moveData(message);
					handleMessageStatus = StatusType.DONE;
					key = "moved";
					value = "data";
					break;
				case CLEAR_STORAGE:
					this.kvServer.clearStorage();
					break;
				case DELETE_STORAGE:
					this.kvServer.deleteStorage();
					break;
				default:
					break;
			}
		} catch (Exception e) {
			logger.error("Unknown error when handling metadata message: " + e.getMessage());
		}
		handleMessage.setMessage(handleMessageStatus.name(), key, value);
		return handleMessage;
	}

	public void run() {
		// while connection is open, listen for messages
		try {
			while (this.isOpen) {
				try {
					JSONMessage receivedMessage = receiveJSONMessage();
					if (receivedMessage != null) {
						JSONMessage sendMessage;
						Metadata metadata = receivedMessage.getMetadata();

						if (metadata == null && this.kvServer.serverStatus == ServerStatus.CLOSED) {
							// If the status is closed , all client requests are responded to with
							// SERVER_STOPPED messages
							sendMessage = new JSONMessage();
							sendMessage.setMessage(StatusType.SERVER_STOPPED.name(), receivedMessage.getKey(),
									receivedMessage.getValue());
						} else if (metadata == null) {
							sendMessage = handleMessage(receivedMessage);
						} else {
							sendMessage = handleMetadataMessage(metadata);
						}

						// In the case of a PUT_MANY, we do not need to send
						if (sendMessage != null) {
							sendJSONMessage(sendMessage);
						}

					}
				} catch (IOException e) {
					logger.error("Server connection lost: " + e);
					this.isOpen = false;
				} catch (Exception e) {
					logger.error("Server connection failed: " + e);
				}
			}
		} finally {
			try {
				// close connection
				if (serverSocket != null) {
					input.close();
					output.close();
					serverSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!" + ioe);
			}
		}
	}
}
