package app_kvClient;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.*;

import shared.messages.JSONMessage;
import shared.messages.TextMessage;

import client.KVStore;

import app_kvServer.KVServer;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements IClientConnection {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) throws Exception {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();

		// this.kvServer = new KVServer(this.clientSocket.getPort(), -1, "");
	}

	@Override
	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();

		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ json.getJSON() + "'");
	}

	@Override
	public JSONMessage receiveJSONMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		System.out.println("RECEIVING");


		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		// logger.info("First Char: " + read);
		// Check if stream is closed (read returns -1)
		// if (read == -1){
		// TextMessage msg = new TextMessage("");
		// return msg;
		// }
		System.out.println("swag");

		while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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
		String jsonStr = json.byteToString(tmp);
		if (jsonStr == null || jsonStr.trim().isEmpty()) {
			// TODO?? null message
			System.out.println("NULL: " + jsonStr);
			return null;
		}
		// deserialize
		System.out.println("ABOUT TO DESERIALIZE");
		json.deserialize(jsonStr);
		logger.info("RECEIVE \t<"
				+ clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '"
				+ json.getJSON().trim() + "'");
		System.out.println("RETURN JSON: " + json);
		
		return json;
	}

}
