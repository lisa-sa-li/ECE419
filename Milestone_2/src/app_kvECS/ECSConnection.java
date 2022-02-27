package app_kvECS;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.InvalidKeyException;

import org.apache.log4j.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

import shared.exceptions.KeyValueTooLongException;
import shared.exceptions.UnexpectedValueException;
import shared.messages.JSONMessage;
import shared.messages.Metadata;
import shared.messages.KVMessage.StatusType;

import app_kvServer.KVServer;
import ecs.ECSNode;
import ecs.ZooKeeperApplication;
import ecs.HeartbeatApplication;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 */
public class ECSConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private String ZK_HEARBEAT_ROOT_PATH = "./heartbeat";

	private Socket ecsSocket;
	private InputStream input;
	private OutputStream output;
	private ECSClient ecsClient;
	private int zkTimeout = 1000;

	private KVServer kvServer;

	private ZooKeeper zk;
	private ZooKeeperApplication zkApp;

	public ECSConnection(Socket ecsSocket, ECSClient ecsClient) throws Exception {
		this.ecsSocket = ecsSocket;
		this.isOpen = true;
		this.ecsClient = ecsClient;
		connect();
	}

	public ECSConnection(String zkHost, int zkPort, String serverName, KVServer kvServer) throws Exception {
		this.kvServer = kvServer;
		// CREATE HEARTBEART

		zkApp = new ZooKeeperApplication(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, zkPort, zkHost);
		try {
			zk = zkApp.connect(zkHost + ":" + String.valueOf(zkPort), zkTimeout);
		} catch (InterruptedException | IOException e) {
			logger.error("Cannot connect to ZK server!", e);
			System.exit(1);
		}

		try {
			if (zk.exists(ZooKeeperApplication.ZK_NODE_ROOT_PATH, false) == null) {
				logger.error("ZK does not exist, has not been initialized yet");
				System.exit(1);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
			System.exit(1);
		} catch (Exception e) {
			logger.error(e);
			System.exit(1);
		}

		try {
			if (zk.exists(ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + serverName, false) == null) {
				logger.error("This node has not been added to ZK yet????");
				System.exit(1);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
			System.exit(1);
		} catch (Exception e) {
			logger.error(e);
			System.exit(1);
		}

		try {
			String heartbeatPath = ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH + "/" + serverName;
			zkApp.create(heartbeatPath, "heartbeat", CreateMode.EPHEMERAL);
			// Set heartbeat here
			zk.exists(heartbeatPath, new HeartbeatApplication(kvServer, zk, serverName));
		} catch (KeeperException | InterruptedException e) {
			logger.error(e);
			System.exit(1);
		} catch (Exception e) {
			logger.error(e);
			System.exit(1);
		}

	}

	public void connect() throws IOException {
		try {
			output = this.ecsSocket.getOutputStream();
			input = this.ecsSocket.getInputStream();

			logger.info("Connected to " + this.ecsSocket.getInetAddress().getHostName() + " on port "
					+ this.ecsSocket.getPort());
		} catch (IOException e) {
			logger.error("Error! Unable to establish server connection. \n", e);
		}
	}

	public void sendJSONMessage(Metadata meta) throws IOException {
		JSONMessage json = new JSONMessage();
		json.setMessage(meta.getStatus().name(), "blah", "blah", meta);
		byte[] jsonBytes = json.getJSONByte();

		output.write(jsonBytes, 0, jsonBytes.length);
		output.flush();
		logger.info("SEND \t<" + ecsSocket.getInetAddress().getHostAddress() + ":" + ecsSocket.getPort() + ">: '"
				+ json.getJSON() + "'");
	}

	public JSONMessage receiveMetadataMessage() throws IOException {
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		// Read first char from stream
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
			logger.debug("jsonStr is null in ServerConnection");
			return null;
		}
		json.deserialize(jsonStr);
		logger.info("RECEIVE \t<" + ecsSocket.getInetAddress().getHostAddress() + ":" + ecsSocket.getPort()
				+ ">: '" + json.getJSON().trim() + "'");
		return json;
	}

	public void run() {
		// while connection is open, listen for messages
		try {
			System.out.println("Runnin in ECSConnection");
			while (this.isOpen) {
				System.out.println("Runnin Biteches");
				try {
					System.out.println("Listening for messages");
					JSONMessage recievedMesage = receiveMetadataMessage();
					System.out.println("got eeem");

					// if (recievedMesage != null) {
					// JSONMessage sendMessage = handleMessage(recievedMesage);
					// sendJSONMessage(sendMessage);
					// }
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
				if (ecsSocket != null) {
					// Send message????
					input.close();
					output.close();
					ecsSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}
	}

}