package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

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
	private InputStream input;
	private OutputStream output;
	private ReplicateServer replicateServer;

	/**
	 * Constructs a new ServerConnection object for a given TCP socket.
	 * 
	 * @param serverSocket the Socket object for the server connection.
	 */
	public ReplicateConnection(Socket master, ReplicateServer replicateServer) throws Exception {
		new ServerLogSetup("logs/serverConnection.log", Level.ALL);

		this.master = master;
		this.isOpen = true;
		this.replicateServer = replicateServer;
		connect();
	}

	@Override
	public void connect() throws IOException {
		try {
			output = this.master.getOutputStream();
			input = this.master.getInputStream();

			logger.info("Connected to " + this.master.getInetAddress().getHostName() + " on port "
					+ this.master.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n" + e);
		}
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();
		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" + master.getInetAddress().getHostAddress() + ":" + master.getPort() + ">: '"
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
		logger.info("RECEIVE \t<" + master.getInetAddress().getHostAddress() + ":" + master.getPort()
				+ ">: '" + json.getJSON().trim() + "'");
		return json;
	}

	private JSONMessage handleMetadataMessage(Metadata message) {
		// value back
		StatusType handleMessageStatus = StatusType.NO_STATUS;
		JSONMessage handleMessage = new JSONMessage();
		String key = "";
		String value = "";

		try {
			switch (message.getStatus()) {
				case INIT_REPLICATE_DATA:
					break;
				case UPDATE_REPLICATE_DATA:
					break;
				case DELETE_REPLICATE_DATA:
					break;
				default:
					break;
			}
		} catch (Exception e) {
			logger.error("Unknown error when handling replicate metadata message: " + e.getMessage());
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

                        sendMessage = handleMetadataMessage(metadata);
                        // do we reply?
                        // sendJSONMessage(sendMessage);
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
				if (master != null) {
					input.close();
					output.close();
					master.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!" + ioe);
			}
        }

    }

}
