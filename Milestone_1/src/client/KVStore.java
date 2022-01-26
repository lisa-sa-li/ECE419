package client;

import shared.messages.KVMessage;
import org.apache.log4j.*;
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
import shared.messages.TextMessage;

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
	String fileName = "storage.txt";
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;


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

	// @Override
	// public KVMessage put(String key, String value) throws Exception {
	// 	// TODO Auto-generated method stub
	// 	// System.out.println(KVMessage.StatusType.PUT);
	// 	// KVMessage req = new KVMessage(KVMessage.StatusType.PUT);
	// 	// System.out.println(req);

	// 	return null;
	// }

	// @Override
	// public KVMessage get(String key) throws Exception {
	// 	// TODO Auto-generated method stub
	// 	return null;
	// }

	 @Override
	public boolean put(String key, String value) throws Exception {
        try {
            // Below is slightly modified logic from https://stackoverflow.com/questions/20039980/java-replace-line-in-text-file
            BufferedReader file = new BufferedReader(new FileReader(this.fileName));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            boolean foundKey = false;
            JSONMessage json;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                keyFromFile = json.getKey();

                // The key already exists in the file, update the old value with the new value
                if (keyFromFile.equals(key)) {
                    foundKey = true;

                    // If value == null, that means to delete so we will skip appending the line
                    // Otherwise, update the value and append to file
                    if (value != null) {
                        json.setValue(value);
                        line = json.serialize();
                        inputBuffer.append(line);
                        inputBuffer.append('\n');
                    }
                    break;
                } 
            }

            // If key does not exist in the file, add it to the end of the file
            if (foundKey == false) {
                json = new JSONMessage();
                json.setMessage("", key, value); // We don't care about status here
                line = json.serialize();
                inputBuffer.append(line);
                inputBuffer.append('\n');
            }

            file.close();

            // Overwrite file with the data
            FileOutputStream fileOut = new FileOutputStream(this.fileName);
            fileOut.write(inputBuffer.toString().getBytes());
            fileOut.close();
            
            logger.info("Completed 'put' operation into storage server");
            return true;
        } catch (Exception e) {
            System.out.println("Problem reading file.");
        }
        return false;
	}

    @Override
	public String get(String key) throws Exception {
        try {
            BufferedReader file = new BufferedReader(new FileReader(this.fileName));
            JSONMessage json;
            String line;
            String keyFromFile;
            boolean foundKey = false;

            while ((line = file.readLine()) != null) {
                // Covert each line to a JSON so we can read the key and value
                json = new JSONMessage();
                json.deserialize(line);
                key = json.getKey();

                // The key exists in the file
                if (keyFromFile.equals(key)) {
                    foundKey = true;
                    break;
                } 
            }
            file.close();
            logger.info("Completed 'get' operation into storage server");

            if (foundKey == true) {
                return json.getValue();
            }
        } catch (Exception e) {
            System.out.println("Problem reading file.");
        }

        // The key does not exist in the file
		return null;
	}

	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '" + msg.getMsg() +"'");
	}

	private TextMessage receiveMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while(read != 13 && reading) {/* carriage return */
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
			/* only read valid characters, i.e. letters and numbers */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
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
		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("Receive message:\t '" + clientSocket.getInetAddress().getHostAddress() + ":"
				+ clientSocket.getPort() + ">: '" + msg.getMsg() + "'");
		return msg;
	}
}
