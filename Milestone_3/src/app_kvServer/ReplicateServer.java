package app_kvServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
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
public class ReplicateServer implements Runnable {
	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private ServerSocket listeningSocket;
	private InputStream input;
	private OutputStream output;
	private KVServer kvServer;

	/**
	 * Constructs a new ServerConnection object for a given TCP socket.
	 * 
	 * @param serverSocket the Socket object for the server connection.
	 */
	public ReplicateServer(ServerSocket listeningSocket, KVServer kvServer) throws Exception {
		new ServerLogSetup("logs/serverConnection.log", Level.ALL);

		this.listeningSocket = listeningSocket;
		this.isOpen = true;
		this.kvServer = kvServer;
	}

	public String getName() {
		return kvServer.serverName;
	}

	public String getServerHost() {
		return kvServer.getHostname();
	}

	public Integer getPort() {
		return listeningSocket.getLocalPort();
	}

	public void run() {
		while (kvServer.isRunning()) {
			try {
				Socket client = listeningSocket.accept();
				ReplicateConnection replicateConnection = new ReplicateConnection(client, this);
				Thread newThread = new Thread(replicateConnection);
				newThread.start();

				logger.info(
						"Connected to " + client.getInetAddress().getHostName() + " on port "
								+ client.getPort());
			} catch (IOException e) {
				logger.error("Error! Unable to establish connection to master. \n", e);
			} catch (Exception e) {
				logger.error("Listener Error! \n", e);
			}

		}
	}
}