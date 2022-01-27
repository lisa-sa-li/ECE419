package client;

import shared.messages.KVMessage;
// import org.apache.log4j.*;
import org.apache.log4j.Logger;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.io.FileOutputStream;
import shared.messages.JSONMessage;

import app_kvClient.ClientConnection;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port    the port of the KVServer
	 */
	String address;
	int port;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private static Logger logger = Logger.getRootLogger();
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private ClientConnection clientConnection;

	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		try {
			Socket clientSocket = new Socket(address, port);
			clientConnection = new ClientConnection(clientSocket);
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			logger.info("Connected to "
					+ clientSocket.getInetAddress().getHostName()
					+ " on port " + clientSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish connection. \n", e);
		}
	}

	@Override
	public void disconnect() {
		logger.info("Tearing down the connection ...");
		try {
			if (clientSocket != null) {
				clientSocket.close();
				clientSocket = null;
				logger.info("Connection closed!");

				// HOW DO WE CLOSE clientConnection?????
			}
		} catch (IOException e) {
			logger.error("Error! Unable to close connection. \n", e);
		}
	}

	@Override
	public JSONMessage put(String key, String value) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage("PUT", key, value);
		this.clientConnection.sendJSONMessage(jsonMessage);

		return this.clientConnection.receiveJSONMessage();
	}

	@Override
	public JSONMessage get(String key) throws Exception {
		JSONMessage jsonMessage = new JSONMessage();
		jsonMessage.setMessage("GET", key, "");
		this.clientConnection.sendJSONMessage(jsonMessage);
		return this.clientConnection.receiveJSONMessage();
	}
}
// public void sendMessage(JSONMessage msg) throws IOException {
// String temp = msg.serialize();
// byte[] msgBytes = msg.stringToByte(temp);
// output.write(msgBytes, 0, msgBytes.length);
// output.flush();
// logger.info("Send message:\t '" +
// clientSocket.getInetAddress().getHostAddress() + ":"
// + clientSocket.getPort() + ">: '" + msg.serialize() +"'");
// }

// private TextMessage receiveMessage() throws IOException {
// int index = 0;
// byte[] msgBytes = null, tmp = null;
// byte[] bufferBytes = new byte[BUFFER_SIZE];

// /* read first char from stream */
// byte read = (byte) input.read();
// boolean reading = true;

// while(read != 13 && reading) {/* carriage return */
// /* if buffer filled, copy to msg array */
// if(index == BUFFER_SIZE) {
// if(msgBytes == null){
// tmp = new byte[BUFFER_SIZE];
// System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
// } else {
// tmp = new byte[msgBytes.length + BUFFER_SIZE];
// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
// BUFFER_SIZE);
// }
// msgBytes = tmp;
// bufferBytes = new byte[BUFFER_SIZE];
// index = 0;
// }
// /* only read valid characters, i.e. letters and numbers */
// if((read > 31 && read < 127)) {
// bufferBytes[index] = read;
// index++;
// }
// /* stop reading is DROP_SIZE is reached */
// if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
// reading = false;
// }
// /* read next char from stream */
// read = (byte) input.read();
// }
// if(msgBytes == null){
// tmp = new byte[index];
// System.arraycopy(bufferBytes, 0, tmp, 0, index);
// } else {
// tmp = new byte[msgBytes.length + index];
// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
// }
// msgBytes = tmp;
// /* build final String */
// TextMessage msg = new TextMessage(msgBytes);
// logger.info("Receive message:\t '" +
// clientSocket.getInetAddress().getHostAddress() + ":"
// + clientSocket.getPort() + ">: '" + msg.getMsg() + "'");
// return msg;
// }

// private JSONMessage receiveJSONMessage() throws IOException {

// int index = 0;
// byte[] msgBytes = null, tmp = null;
// byte[] bufferBytes = new byte[BUFFER_SIZE];

// /* read first char from stream */
// byte read = (byte) input.read();
// boolean reading = true;

// // logger.info("First Char: " + read);
// // Check if stream is closed (read returns -1)
// // if (read == -1){
// // TextMessage msg = new TextMessage("");
// // return msg;
// // }

// while(/*read != 13 && */ read != 10 && read !=-1 && reading) {/* CR, LF,
// error */
// /* if buffer filled, copy to msg array */
// if(index == BUFFER_SIZE) {
// if(msgBytes == null){
// tmp = new byte[BUFFER_SIZE];
// System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
// } else {
// tmp = new byte[msgBytes.length + BUFFER_SIZE];
// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
// BUFFER_SIZE);
// }

// msgBytes = tmp;
// bufferBytes = new byte[BUFFER_SIZE];
// index = 0;
// }

// /* only read valid characters, i.e. letters and constants */
// bufferBytes[index] = read;
// index++;

// /* stop reading is DROP_SIZE is reached */
// if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
// reading = false;
// }

// /* read next char from stream */
// read = (byte) input.read();
// }

// if(msgBytes == null){
// tmp = new byte[index];
// System.arraycopy(bufferBytes, 0, tmp, 0, index);
// } else {
// tmp = new byte[msgBytes.length + index];
// System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
// }

// msgBytes = tmp;

// /* build final Object */
// JSONMessage json = new JSONMessage();
// // bytes to string
// String jsonStr = json.byteToString(tmp);
// // deserialize
// json.deserialize(jsonStr);
// logger.info("RECEIVE \t<"
// + clientSocket.getInetAddress().getHostAddress() + ":"
// + clientSocket.getPort() + ">: '"
// + json.getJSON().trim() + "'");
// return json;
// }
