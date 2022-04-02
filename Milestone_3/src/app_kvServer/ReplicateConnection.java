package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.io.EOFException;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import logging.ServerLogSetup;
import shared.messages.JSONMessage;
import shared.messages.Metadata;
import shared.messages.KVMessage.StatusType;

public class ReplicateConnection implements IServerConnection, Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket master;
	private KVServer kvServer;
	private InputStream input;
	private OutputStream output;
	private ReplicateServer replicateServer;
	public Replicate replicate;

	private ObjectOutputStream oos;
	ObjectInputStream ois;

	/**
	 * Constructs a new ServerConnection object for a given TCP socket.
	 * 
	 * @param serverSocket the Socket object for the server connection.
	 */
	public ReplicateConnection(Socket master, ReplicateServer replicateServer, KVServer kvServer) throws Exception {
		new ServerLogSetup("logs/replicateConnection.log", Level.ALL);

		this.master = master;
		this.kvServer = kvServer;
		this.isOpen = true;
		this.replicateServer = replicateServer;

		// establish itself as a replicate
		this.replicate = new Replicate(replicateServer.getName(), replicateServer.getPort(),
				replicateServer.getServerHost());
		// set master info (name to come later in init message)
		replicate.setMasterPort(master.getPort());
		replicate.setMasterHost(master.getInetAddress().getHostAddress());

		// connect
		connect();
	}

	@Override
	public void connect() throws IOException {
		try {
			output = this.master.getOutputStream();
			input = this.master.getInputStream();

			// oos = new ObjectOutputStream(output);
			// oos.flush();
			// ois = new ObjectInputStream(input);

			logger.info("ReplicateConnection connected to controller " + this.master.getInetAddress().getHostName()
					+ " on port " + this.master.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n" + e);
		}
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();
		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" + master.getInetAddress().getHostAddress() + ":" +
				master.getPort() + ">: '"
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
			// JSONMessage json = new JSONMessage();
			// json.setMessage(StatusType.DISCONNECTED.name(), "disconnected", "in
			// ReplicateConnection");
			return null;
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
		logger.info("RECEIVE from controller \t<" +
				master.getInetAddress().getHostAddress() + ":" + master.getPort()
				+ ">: '" + json.getJSON().trim() + "'");
		return json;

		// // oos = new ObjectOutputStream(output);
		// // oos.flush();
		// ois = new ObjectInputStream(input);

		// String jsonStr = null;
		// try {
		// jsonStr = (String) ois.readObject();
		// } catch (Exception e) {
		// logger.error("Unable to read input stream in replicate connection: " + e);
		// }

		// if (jsonStr == null || jsonStr.trim().isEmpty()) {
		// return null;
		// }

		// // ois.close();

		// JSONMessage json = new JSONMessage();
		// json.deserialize(jsonStr);
		// logger.info("RECEIVE from controller \t<" +
		// master.getInetAddress().getHostAddress() + ":" + master.getPort()
		// + ">: '" + json.getJSON().trim() + "'");
		// return json;

	}

	private void handleMessage(JSONMessage msg) {
		logger.info("IN REPLICATE HANDLE MESSAGE");
		String key = msg.getKey();
		String value = msg.getValue();
		StatusType status = msg.getStatus();

		try {
			switch (status) {
				case INIT_REPLICATE_DATA:
					// logger.info(">>INIT_REPLICATE_DATA");
					replicate.initReplicateData(value);
					// add to KVServer replicate list
					kvServer.addActingReplicate(replicate);
					break;
				case UPDATE_REPLICATE_DATA:
					logger.info(">>UPDATE_REPLICATE_DATA");
					replicate.updateReplicateData(value);
					logger.info(">>Done replicate data update on replicate side");
					break;
				case DELETE_REPLICATE_DATA:
					// logger.info(">>DELETE_REPLICATE_DATA");
					replicate.deleteReplicateData(value);
					// remove from KVServer
					kvServer.removeActingReplicate(replicate);
					break;
				default:
					break;
			}
		} catch (Exception e) {
			logger.error(
					"Unknown error when handling replicate metadata message in ReplicateConnection: " + e.getMessage());
		}
	}

	public void run() {
		// while connection is open, listen for messages
		try {
			while (this.isOpen) {
				try {
					JSONMessage receivedMessage = receiveJSONMessage();

					if (receivedMessage != null) {
						logger.info("Received message not null: " + receivedMessage);
						JSONMessage sendMessage;
						Metadata metadata = receivedMessage.getMetadata();
						// logger.info("swag");
						handleMessage(receivedMessage);
						// do we reply?
						// sendJSONMessage(sendMessage);
					}

				} catch (EOFException e) {
					logger.error("ReplicateConnection EOF " + e);
					// this.isOpen = false;
				} catch (IOException e) {
					logger.error("ReplicateConnection connection lost: " + e);
					this.isOpen = false;
				} catch (Exception e) {
					logger.error("ReplicateConnection connection failed: " + e);
				}
			}
		} finally {
			try {
				// close connection
				if (master != null) {
					input.close();
					output.close();
					// oos.close();
					// ois.close();

					master.close();

				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!" + ioe);
			}
		}

	}

}
