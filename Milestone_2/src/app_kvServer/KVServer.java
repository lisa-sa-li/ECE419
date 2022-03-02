package app_kvServer;

import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.OutputStream;
import java.io.IOException;
import java.net.BindException;
import java.util.*;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

import com.google.gson.Gson;

import shared.messages.KVMessage.StatusType;
import shared.messages.Metadata;
import shared.messages.JSONMessage;

import logging.LogSetup;
import app_kvServer.PersistantStorage;
import app_kvServer.ServerConnection;
import app_kvECS.ECSConnection;
import ecs.ECSNode;
import ecs.ZooKeeperApplication;
import ecs.HeartbeatApplication;
import cache.Cache;
import java.lang.reflect.Constructor;

public class KVServer implements IKVServer, Runnable {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private PersistantStorage persistantStorage;
	private Socket client;
	private boolean running;
	private String hostName;
	private ArrayList<Thread> threads;
	private ServerStatus serverStatus;
	private ServerSocket serverSocket;

	private ECSConnection ecsConnection;
	private String serverName;
	private String zkHost;
	private int zkPort;

	private int zkTimeout = 1000;
	private ZooKeeper zk;
	private ZooKeeperApplication zkApp;

	// hashring variables
	private BigInteger hash;
	private BigInteger endHash;
	private HashMap<String, BigInteger> hashRing;

	// caching variables
	private int cacheSize;
	private CacheStrategy cacheAlgo;
	private Cache cache;

	/**
	 * Start KV Server at given port
	 * 
	 * @param port      given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *                  to keep in-memory
	 * @param algo  specifies the cache replacement strategy in case the cache
	 *                  is full and there is a GET- or PUT-request on a key that is
	 *                  currently not contained in the cache. Options are "FIFO",
	 *                  "LRU", and "LFU".
	 */
	public KVServer(int port, int cacheSize, String algo) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(algo);
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();
		if (this.cacheAlgo == CacheStrategy.None) {
			this.cache = null;
		} else { // allocate cache
			try {
				Constructor<?> constructorCache = Class.forName("cache." + this.cacheAlgo + "Cache").getConstructor(Integer.class);
				this.cache = (Cache) constructorCache.newInstance(cacheSize);
			} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				logger.error(e);
			}
		}
		logger.debug("HERE1");
	}

	public KVServer(int port, String serverName, String zkHost, int zkPort) throws InterruptedException, KeeperException {
		this.port = port;
		this.serverName = serverName;
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		this.zkHost = zkHost;
		this.zkPort = zkPort;

		/*
		// String zkRootPath = ZooKeeperApplication.ZK_ROOT_PATH + "/" + serverName; // ZK_SERVER_ROOT = "/kv_servers"
		// cache setup using information received from Zookeeper node
		try {
			// If there is cache info in zookeeper, retrieve it here and initialize cache
			// TO DO
		} catch (InterruptedException | KeeperException e) {
			logger.error("Unable to retrieve cache info from zookeeper node, so initialize cache with default values");
			this.cacheAlgo = CacheStrategy.FIFO;
			this.cacheSize = 100;
		}
		if (this.cacheAlgo == CacheStrategy.None) {
			this.cache = null;
		} else { // allocate cache
			try {
				Constructor<?> constructorCache = Class.forName("cache." + this.cacheAlgo + "Cache").getConstructor(Integer.class);
				this.cache = (Cache) constructorCache.newInstance(cacheSize);
			} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				logger.error(e);
			}
		}
		*/

		logger.debug("HERE2");
		initHeartbeat();
	}

	public KVServer(int port, int cacheSize, String algo, boolean test) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(algo);
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();
		if (this.cacheAlgo == CacheStrategy.None) {
			this.cache = null;
		} else { // allocate cache
			try {
				Constructor<?> constructorCache = Class.forName("cache." + this.cacheAlgo + "Cache").getConstructor(Integer.class);
				this.cache = (Cache) constructorCache.newInstance(cacheSize);
			} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
				logger.error(e);
			}
		}
		if (test) {
			Thread testThread = new Thread(this);
			testThread.start();
		}
	}

	public void initHeartbeat() {
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
			if (zk.exists(ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + serverName,
					false) == null) {
				logger.error("This node has not been added to ZK yet????");
				System.exit(1);
			}
		} catch (KeeperException | InterruptedException e) {
			logger.error("Cannot check if ZK node for server " + serverName + " already exists", e);
			System.exit(1);
		} catch (Exception e) {
			logger.error("Cannot check if ZK node for server " + serverName + " already exists", e);
			System.exit(1);
		}

		try {
			String heartbeatPath = ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH + "/" +
					serverName;
			zkApp.create(heartbeatPath, "heartbeat", CreateMode.EPHEMERAL);
			// Set heartbeat here
			zk.exists(heartbeatPath, new HeartbeatApplication(this, zk, serverName));
		} catch (KeeperException | InterruptedException e) {
			logger.error("Cannot create heartbeat for server " + serverName, e);
			System.exit(1);
		} catch (Exception e) {
			logger.error("Cannot create heartbeat for server " + serverName, e);
			System.exit(1);
		}
	}

	public void initKVServer(String metadata) {
		// Initialize the KVServer with the metadata and block it for client requests,
		// i.e., all client requests are rejected with an SERVER_STOPPED error message;
		// ECS requests have to be processed.

	}

	@Override
	public void start() {
		this.serverStatus = ServerStatus.OPEN;
	}

	@Override
	public void stop() {
		this.serverStatus = ServerStatus.CLOSED;
	}

	@Override
	public void shutDown() {
		this.serverStatus = ServerStatus.SHUTDOWN;
	}

	@Override
	public void lockWrite() {
		this.serverStatus = ServerStatus.LOCKED;
	}

	@Override
	public void unLockWrite() {
		this.serverStatus = ServerStatus.OPEN;
	}

	public boolean inHashRing() {
		logger.info("is " + this.serverName + " in hashring " + hashRing.keySet() + "?: "
				+ hashRing.get(this.serverName) != null);
		return (hashRing.get(this.serverName) != null);
	}

	public Boolean getHashRange() {
		if (inHashRing() == false) {
			// This detects that it's not in the hashring anymore, thus it needs to die
			this.hash = null;
			this.endHash = null;
			return true;
		}

		Collection<BigInteger> keys = hashRing.values();
		ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
		Collections.sort(orderedKeys);

		this.hash = hashRing.get(this.serverName);
		if (orderedKeys.size() == 1) {
			this.endHash = null;
			return false;
		}
		Integer nextIdx = orderedKeys.indexOf(this.hash);
		nextIdx = (nextIdx + 1) % orderedKeys.size();

		this.endHash = orderedKeys.get(nextIdx);
		return false;
	}

	public void moveData(Metadata metadata) { // range, server
		update(metadata);

		// figoure out new endHash HERE
		Boolean die = getHashRange();

		// Transfer a subset (range) of the KVServer's data to another KVServer
		// (reallocation before removing this server or adding a new KVServer to the
		// ring);
		// send a notification to the ECS, if data transfer is completed.
		lockWrite();

		ECSNode receiverNode = metadata.getReceiverNode();
		String hostOfReceiver = receiverNode.getNodeHost();
		int portOfReceiver = receiverNode.getNodePort();
		String nameOfReceiver = receiverNode.getNodeName();
		BigInteger hash = receiverNode.getHash();
		BigInteger endHash = receiverNode.getEndHash();

		// logger.debug("> " + hostOfReceiver + nameOfReceiver + "?" + hash + "?" +
		// endHash);

		if (nameOfReceiver == this.serverName) {
			unLockWrite();
			return;
		}

		try {
			// This is the data being remove from this server and being moved to the
			// reciever server
			logger.debug("BEFORE GET DATA IN RANGE die " + die);
			String dataInRange = persistantStorage.getDataInRange(hash, endHash, die);
			logger.debug("AFTER GET DATA IN RANGE " + dataInRange);

			Socket socket = new Socket(hostOfReceiver, portOfReceiver);
			OutputStream output = socket.getOutputStream();

			JSONMessage json = new JSONMessage();
			json.setMessage(StatusType.PUT_MANY.name(), "put_many", dataInRange, null);
			byte[] jsonBytes = json.getJSONByte();

			output.write(jsonBytes, 0, jsonBytes.length);
			output.flush();
			output.close();
			logger.info("Send data to node " + nameOfReceiver);
		} catch (Exception e) {
			logger.error("Unable to send data to node " + nameOfReceiver + ", " + e);
		}

		unLockWrite();
		if (die == true) {
			try {
				TimeUnit.SECONDS.sleep(5);
				kill();
			} catch (Exception e) {
				logger.error("Unable to kill server");
			}
		}
	}

	public void update(Metadata metadata) {
		// Update the metadata repository of this server
		this.hashRing = metadata.order;
		Boolean tmp = getHashRange();
	}

	public boolean isMe(BigInteger toHash) {
		if (this.endHash == null) {
			// only server in the ring
			return true;
		}

		int isEndHashLarger = this.endHash.compareTo(this.hash);
		// a.compareTo(b)
		// 0 = equal
		// 1 = a > b
		// -1 = a < b

		int left = this.hash.compareTo(toHash);
		int right = this.endHash.compareTo(toHash);

		if (isEndHashLarger > 0) {
			// left = 12, right = 20, tohash 18
			return (left <= 0 && right > 0);
		} else {
			// left = 99, right = 2, tohash 1
			return (left <= 0 || right > 0);
		}
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		return serverSocket.getInetAddress().getHostName();
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return this.cacheAlgo;
	}

	@Override
	public int getCacheSize() {
		return this.cacheSize;
	}

	@Override
	public boolean inStorage(String key) throws Exception {
		return this.persistantStorage.inStorage(key);
	}

	@Override
	public boolean inCache(String key) {
		if (this.cache != null) {
			return this.cache.containsKey(key);
		} else {
			return false;
		}
	}

	@Override
	public String getKV(String key) throws Exception {
		if (this.cache != null) {
			if (this.inCache(key)) {
				return this.cache.get(key);
			} else {
				// The key is not in cache, so read from persistent storage and update cache
				String value = this.persistantStorage.get(key);
				if (value != null) {
					this.cache.put(key, value);
				} else {
					logger.warn("Key " + key + " is not found");
					throw new Exception("Key is not found");
				}
				return value;
			}
		} else {
			String value = this.persistantStorage.get(key);
			if (value == null) {
				logger.warn("Key " + key + " is not found");
				throw new Exception("Key is not found");
			}
			return value;
		}
	}

	@Override
	public StatusType putKV(String key, String value) throws Exception {
		if (this.cache != null){
			this.cache.put(key, value);
		}
		return this.persistantStorage.put(key, value);
	}

	public StatusType appendToStorage(String keyValues) throws Exception {
		return this.persistantStorage.appendToStorage(keyValues);
	}

	@Override
	public void clearCache() {
		if (this.cache != null) {
			this.cache.clear();
		}
		logger.info("Cache cleared");
	}

	@Override
	public void clearStorage() {
		this.persistantStorage.clearStorage();
		if (this.cache != null){
			this.cache.clear();
		}
	}

	@Override
	public void deleteStorage() {
		this.persistantStorage.deleteStorage();
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: " + serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if (e instanceof BindException) {
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	@Override
	public void run() {
		running = initializeServer();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					client = serverSocket.accept();
					ServerConnection serverConnection = new ServerConnection(client, this);
					Thread newThread = new Thread(serverConnection);
					newThread.start();
					this.threads.add(newThread);

					logger.info(
							"Connected to " + client.getInetAddress().getHostName() + " on port "
									+ client.getPort());
				} catch (IOException e) {
					logger.error("Error! Unable to establish connection to server. \n", e);
				} catch (Exception e) {
					logger.error("Error! \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.running;
	}

	@Override
	public void kill() {
		running = false;
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
				logger.error("Error! Unable to close client socket on port: " + port, e);
			}
		}
		if (serverSocket != null) {
			try {
				serverSocket.close();
			} catch (IOException e) {
				logger.error("Error! Unable to close socket on port: " + port, e);
			}
		}
		System.exit(1);
	}

	@Override
	public void close() {
		running = false;
		try {
			// find + stop all threads
			for (int i = 0; i < threads.size(); i++) {
				threads.get(i).interrupt();
			}
		} catch (Exception e) {
			logger.error("Error! Unable to interrupt threads: " + e);
		}
		kill();
	}

	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if (args.length != 1 && args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else if (args.length == 4) {
				int port = Integer.parseInt(args[0]);
				String serverName = args[1];
				String zkHost = args[2];
				int zkPort = Integer.parseInt(args[3]);
				new KVServer(port, serverName, zkHost, zkPort).run();
			} else {
				int port = Integer.parseInt(args[0]);
				// System.out.println("args", args);
				// TODO: set params for cache size and strategy
				new KVServer(port, 1, "").run();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		} catch (Exception e) {
			System.out.println("Error! Something went wrong!");
			System.exit(1);
		}
	}
}
