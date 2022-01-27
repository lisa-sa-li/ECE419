package app_kvServer;

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
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) throws Exception {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		// this.kvServer = new KVServer(this.clientSocket.getPort(), -1, "");
	}
	
	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	// public void run() {
	// 	System.out.print("hhiii");

	// 	KVStore kvStore = new KVStore("temp", 123);
	// 	String key;
	// 	String value;
	// 	JSONMessage latestMsg;
	// 	String kvStoreValue;
		
	// 	try {
	// 		output = clientSocket.getOutputStream();
	// 		input = clientSocket.getInputStream();
		
	// 		sendTextMessage(new TextMessage(
	// 				"Connection to MSRG Echo server established: " 
	// 				+ clientSocket.getLocalAddress() + " / "
	// 				+ clientSocket.getLocalPort()));
			
	// 		while(isOpen) {
	// 			try {
	// 				// TextMessage latestMsg = receiveMessage();
	// 				// sendTextMessage(latestMsg);
	// 				latestMsg = receiveJSONMessage();
	// 				key = latestMsg.getKey();
	// 				value = latestMsg.getValue();

	// 				// TODO add PUT, GET logic here
	// 				if (latestMsg.getStatus().name() == "put") {
	// 					try {
	// 						this.kvServer.putKV(key, value);
	// 						sendTextMessage(new TextMessage("Successfully put key:" + key + ", value: " + value));
	// 					} catch (Exception e) {
	// 						sendTextMessage(new TextMessage("Failed to put key:" + key + ", value: " + value));
	// 					}
	// 				} else if (latestMsg.getStatus().name() == "get") {
	// 					try {
	// 						kvStoreValue = this.kvServer.getKV(key);
	// 						// kvStoreValue = kvStore.get(key);
	// 						// TextMessage txt = kvStore.get(key);
	// 						sendTextMessage(new TextMessage("Successfully get key:" + key + ", value is: " + kvStoreValue));
	// 					} catch (Exception e) {
	// 						sendTextMessage(new TextMessage("Failed to get key:" + key));
	// 					}
	// 				}
					
	// 			/* connection either terminated by the client or lost due to 
	// 			 * network problems*/	
	// 			} catch (IOException ioe) {
	// 				logger.error("Error! Connection lost!");
	// 				isOpen = false;
	// 			} catch (Exception e) {
	// 				logger.error("Error! Connection lost!");
	// 				isOpen = false;
	// 			}				
	// 		}
			
	// 	} catch (IOException ioe) {
	// 		logger.error("Error! Connection could not be established!", ioe);
			
	// 	} finally {
			
	// 		try {
	// 			if (clientSocket != null) {
	// 				input.close();
	// 				output.close();
	// 				clientSocket.close();
	// 			}
	// 		} catch (IOException ioe) {
	// 			logger.error("Error! Unable to tear down connection!", ioe);
	// 		}
	// 	}
	// }
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	// public void sendTextMessage(TextMessage msg) throws IOException {
	// 	byte[] msgBytes = msg.getMsgBytes();
	// 	output.write(msgBytes, 0, msgBytes.length);
	// 	output.flush();
	// 	logger.info("SEND \t<" 
	// 			+ clientSocket.getInetAddress().getHostAddress() + ":" 
	// 			+ clientSocket.getPort() + ">: '" 
	// 			+ msg.getMsg() +"'");
    // }

	public void sendJSONMessage(JSONMessage json) throws IOException {
		byte[] jsonBytes = json.getJSONByte();
		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ json.getJSON() +"'");
    }

// 	private TextMessage receiveTextMessage() throws IOException {
		
// 		int index = 0;
// 		byte[] msgBytes = null, tmp = null;
// 		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
// 		/* read first char from stream */
// 		byte read = (byte) input.read();	
// 		boolean reading = true;
		
// //		logger.info("First Char: " + read);
// //		Check if stream is closed (read returns -1)
// //		if (read == -1){
// //			TextMessage msg = new TextMessage("");
// //			return msg;
// //		}

// 		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
// 			/* if buffer filled, copy to msg array */
// 			if(index == BUFFER_SIZE) {
// 				if(msgBytes == null){
// 					tmp = new byte[BUFFER_SIZE];
// 					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
// 				} else {
// 					tmp = new byte[msgBytes.length + BUFFER_SIZE];
// 					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// 					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
// 							BUFFER_SIZE);
// 				}

// 				msgBytes = tmp;
// 				bufferBytes = new byte[BUFFER_SIZE];
// 				index = 0;
// 			} 
			
// 			/* only read valid characters, i.e. letters and constants */
// 			bufferBytes[index] = read;
// 			index++;
			
// 			/* stop reading is DROP_SIZE is reached */
// 			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
// 				reading = false;
// 			}
			
// 			/* read next char from stream */
// 			read = (byte) input.read();
// 		}
		
// 		if(msgBytes == null){
// 			tmp = new byte[index];
// 			System.arraycopy(bufferBytes, 0, tmp, 0, index);
// 		} else {
// 			tmp = new byte[msgBytes.length + index];
// 			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
// 			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
// 		}
		
// 		msgBytes = tmp;
		
// 		/* build final String */
// 		TextMessage msg = new TextMessage(msgBytes);
// 		logger.info("RECEIVE \t<" 
// 				+ clientSocket.getInetAddress().getHostAddress() + ":" 
// 				+ clientSocket.getPort() + ">: '" 
// 				+ msg.getMsg().trim() + "'");
// 		return msg;
//     }
	
	
	public JSONMessage receiveJSONMessage() throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

		while(/*read != 13  && */ read != 10 && read !=-1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
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
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
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
		// deserialize
		json.deserialize(jsonStr);
		logger.info("RECEIVE \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ json.getJSON().trim() + "'");
		return json;
    }
	

	
}
