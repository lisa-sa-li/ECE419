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
import com.google.gson.Gson;
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

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

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

	ObjectInputStream ois;
	ObjectOutputStream oos;

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

			// oos = new ObjectOutputStream(output);
			// ois = new ObjectInputStream(input);

			logger.info(
					"Connected to ServerConnection " + this.serverSocket.getInetAddress().getHostName() + " on port "
							+ this.serverSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n" + e);
		}
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		// byte[] jsonBytes = json.getJSONByte();
		// output.write(jsonBytes, 0, jsonBytes.length);
		// output.flush();

		oos = new ObjectOutputStream(output);

		String msgText = json.serializeMsg();
		oos.writeObject(msgText);
		oos.flush();
		// oos.close();

		logger.info("SEND \t<" + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort() + ">: '"
				+ json.getJSON() + "'");
	}

	@Override
	public JSONMessage receiveJSONMessage() throws IOException {
		// int index = 0;
		// byte[] msgBytes = null, tmp = null;
		// byte[] bufferBytes = new byte[BUFFER_SIZE];

		// byte read = (byte) input.read();
		// boolean reading = true;

		// // Check if stream is closed (read returns -1)
		// if (read == -1) {
		// JSONMessage json = new JSONMessage();
		// json.setMessage(StatusType.DISCONNECTED.name(), "disconnected",
		// kvServer.getNamePortHost());
		// return json;
		// }

		// int endChar = 0;
		// while (reading && endChar < 3 && read != -1) {
		// // Keep a count of EOMs to know when to stop reading
		// // 13 = CR, 10 = LF/NL
		// if (read == 13 || read == 10) {
		// endChar++;
		// }

		// // If buffer filled, copy to msg array
		// if (index == BUFFER_SIZE) {
		// if (msgBytes == null) {
		// tmp = new byte[BUFFER_SIZE];
		// System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
		// } else {
		// tmp = new byte[msgBytes.length + BUFFER_SIZE];
		// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
		// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
		// }

		// msgBytes = tmp;
		// bufferBytes = new byte[BUFFER_SIZE];
		// index = 0;
		// }

		// // Only read valid characters, i.e. letters and constants
		// bufferBytes[index] = read;
		// index++;

		// // Stop reading is DROP_SIZE is reached
		// if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
		// reading = false;
		// }

		// // Read next char from stream
		// read = (byte) input.read();
		// }

		// if (msgBytes == null) {
		// tmp = new byte[index];
		// System.arraycopy(bufferBytes, 0, tmp, 0, index);
		// } else {
		// tmp = new byte[msgBytes.length + index];
		// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
		// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		// }

		// msgBytes = tmp;

		// // Build final Object and convert from bytes to string
		// JSONMessage json = new JSONMessage();
		// String jsonStr = json.byteToString(msgBytes);

		// if (jsonStr == null || jsonStr.trim().isEmpty()) {
		// return null;
		// }

		// json.deserialize(jsonStr);
		// logger.info("RECEIVED from client/ecs/server \t<" +
		// serverSocket.getInetAddress().getHostAddress() + ":"
		// + serverSocket.getPort()
		// + ">: '" + json.getJSON().trim() + "'");
		// return json;

		// oos = new ObjectOutputStream(output);
		ois = new ObjectInputStream(input);

		String jsonStr = null;
		try {
			jsonStr = (String) ois.readObject();
		} catch (Exception e) {
			logger.error("Unable to read input stream in server connection: " + e);
		}

		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			return null;
		}

		// ois.close();

		JSONMessage json = new JSONMessage();
		json.deserializeMsg(jsonStr);
		logger.info("RECEIVED from client/ecs/server \t<" + serverSocket.getInetAddress().getHostAddress() + ":"
				+ serverSocket.getPort()
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

		HashMap<String, BigInteger> order = new HashMap<String, BigInteger>();

		switch (status) {
			case PUT:
				try {
					if (this.kvServer.hash != null && !this.kvServer.isMe(key)) {
						handleMessageStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						// send back metadata
						order = this.kvServer.getOrder();
						break;
					}
					// check key, value length
					if (key.length() > 20) {
						throw new KeyValueTooLongException("Key too long: " + key);
					}
					if (key.trim().isEmpty() || key == null) {
						throw new InvalidKeyException("Invalid key: " + key);
					}
					if (value.length() > 120000) {
						throw new KeyValueTooLongException("Value too long : " + value);
					}
					handleMessageStatus = this.kvServer.putKV(key, value);
					// logger.info(handleMessageStatus.name() + ": key " + key + " & value " + value);
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
					// logger.info("PUT_MANY success");
				} catch (Exception e) {
					handleMessageStatus = StatusType.PUT_ERROR;
					logger.info("PUT_ERROR: key " + key + " & value " + value);
				}
				break;
			case GET:
				String replicaName = null;
				if (this.kvServer.hash != null && !this.kvServer.isMe(key)) {
					// Returns the replica whose hash range this key falls in;
					replicaName = this.kvServer.keyInReplicasRange(key);
					if (replicaName == null) {
						handleMessageStatus = StatusType.SERVER_NOT_RESPONSIBLE;
						order = this.kvServer.getOrder();
						// send back metadata
						break;
					}
				}
				try {
					if (key.length() > 20) {
						throw new KeyValueTooLongException("Key too long: " + key);
					}
					if (!value.trim().isEmpty() && value != null) {
						throw new UnexpectedValueException("Unexpected value for GET: " + value);
					}
					if (key.trim().isEmpty() || key == null) {
						throw new InvalidKeyException("Invalid key: " + key);
					}
					// Determine if we should get from the controller or replica
					if (replicaName != null) {
						handleMessageValue = this.kvServer.getKVFromReplica(key, replicaName);
					} else {
						handleMessageValue = this.kvServer.getKV(key);
					}

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
				// logger.info("Client is disconnected");
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
				case RECOVER:
					this.kvServer.moveReplicateData(message);
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
					logger.debug("MESSAGE RECEIVED: " + receivedMessage);
					if (receivedMessage != null) {
						JSONMessage sendMessage;
						logger.info(">1");
						Metadata metadata = receivedMessage.getMetadata();
						logger.info(">2");
						if (metadata == null) {
							sendMessage = new JSONMessage();
							ServerStatus serverStatus = this.kvServer.serverStatus;
							// logger.debug("serverStatus: " + serverStatus);
							if (serverStatus == ServerStatus.CLOSED) {
								// If the status is closed, all client requests are responded to with
								// SERVER_STOPPED messages
								sendMessage.setMessage(StatusType.SERVER_STOPPED.name(), receivedMessage.getKey(),
										receivedMessage.getValue());
							} else if (serverStatus == ServerStatus.LOCKED
									&& receivedMessage.getStatus() == StatusType.PUT) {
								// If the status is write locked, only get is possible
								sendMessage.setMessage(StatusType.SERVER_WRITE_LOCK.name(), receivedMessage.getKey(),
										receivedMessage.getValue());
							} else {
								// logger.debug("IN ELSE");
								sendMessage = handleMessage(receivedMessage);
								logger.debug("handled message key: " + sendMessage.getKey() + " : " +
										sendMessage.getValue() + " : " + sendMessage.getStatus());
							}
						} else {
							sendMessage = handleMetadataMessage(metadata);
							// logger.debug("sendMessage: " + sendMessage);
						}
						// In the case of a PUT_MANY, we do not need to send a message
						if (sendMessage != null) {
							logger.debug("sending sendMessage: " + sendMessage);
							sendJSONMessage(sendMessage);
							// logger.debug("sent success");
						}
						logger.debug("this.isOpen? : " + this.isOpen);
					}
				} catch (IOException e) {
					logger.error("ServerConnection connection lost: " + e);
					this.isOpen = false;
				} catch (Exception e) {
					logger.error("ServerConnection connection failed: " + e);
				}
			}
		} catch (Exception e) {
			logger.error("-----SOMETHING IS SEVERELY WRONG HERE-----");
		} finally {
			try {
				// close connection
				if (serverSocket != null) {

					input.close();
					output.close();
					serverSocket.close();
					oos.close();
					ois.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!" + ioe);
			}
		}
	}
}
