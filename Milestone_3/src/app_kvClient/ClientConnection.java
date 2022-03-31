package app_kvClient;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.JSONMessage;
import shared.messages.KVMessage;
import shared.messages.TextMessage;
import shared.messages.KVMessage.StatusType;

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 */
public class ClientConnection implements IClientConnection {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	ObjectInputStream ois;
	ObjectOutputStream oos;

	public ClientConnection(Socket clientSocket) throws Exception {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();

		// oos = new ObjectOutputStream(output);
		// oos.flush();
		// ois = new ObjectInputStream(input);
	}

	@Override
	public void sendJSONMessage(JSONMessage kvJson) throws IOException {
		// byte[] jsonBytes = kvJson.getJSONByte();
		// output.write(jsonBytes, 0, jsonBytes.length);
		// output.flush();

		oos = new ObjectOutputStream(output);
		oos.flush();

		String msgText = kvJson.serializeMsg();
		oos.writeObject(msgText);
		oos.flush();
		// oos.reset();
		// oos.close();

		logger.info(
				"SEND \t<" + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + ">: '"
						+ kvJson.getJSON() + "'");
	}

	@Override
	public JSONMessage receiveJSONMessage() throws IOException {
		// int index = 0;
		// byte[] msgBytes = null, tmp = null;
		// byte[] bufferBytes = new byte[BUFFER_SIZE];

		// // Read first char from stream
		// byte read = (byte) input.read();
		// boolean reading = true;

		// // Check if stream is closed (read returns -1)
		// if (read == -1) {
		// JSONMessage json = new JSONMessage();
		// json.setMessage(StatusType.DISCONNECTED.name(), "disconnected", "in
		// ClientConnection", null);
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

		// // Stop reading is DROP_SIZE is reached */
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
		// logger.debug("jsonStr is null in ClientConnection");
		// return null;
		// }
		// json.deserialize(jsonStr);
		// logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() +
		// ":" + clientSocket.getPort()
		// + ">: '" + json.getJSON().trim() + "'");
		// return json;

		// oos = new ObjectOutputStream(output);
		ois = new ObjectInputStream(input);

		String jsonStr = null;
		try {
			jsonStr = (String) ois.readObject();
		} catch (Exception e) {
			logger.error("Unable to read input stream in client connection: " + e);
		}

		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			return null;
		}
		// ois.close();

		JSONMessage json = new JSONMessage();
		json.deserializeMsg(jsonStr);
		logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() +
				":" + clientSocket.getPort()
				+ ">: '" + json.getJSON().trim() + "'");

		return json;

	}

	public void close() throws IOException {
		try {
			if (clientSocket != null) {
				clientSocket.close();
			}
			if (input != null) {
				input.close();
				ois.close();
			}
			if (output != null) {
				output.close();
				oos.close();
			}
		} catch (IOException ioe) {
			logger.error("Error! Unable to tear down connection!", ioe);
		}
	}
}
