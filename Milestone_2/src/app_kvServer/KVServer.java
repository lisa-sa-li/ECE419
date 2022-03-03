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
import shared.Utils;

import logging.LogSetup;
import app_kvServer.PersistantStorage;
import app_kvServer.ServerConnection;
import app_kvECS.ECSConnection;
import ecs.ECSNode;
import ecs.HashRing;
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
	public ServerStatus serverStatus;
	private ServerSocket serverSocket;

	private ECSConnection ecsConnection;
	public String serverName;
	private String zkHost;
	private int zkPort;

	private int zkTimeout = 1000;
	private ZooKeeper zk;
	private ZooKeeperApplication zkApp;

	// hashring variables
	private BigInteger hash;
	private BigInteger endHash;
	private HashMap<String, BigInteger> hashRing;
	// private HashRing hashRingClass = new HashRing();

	private Utils utils = new Utils();

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
	 * @param algo      specifies the cache replacement strategy in case the cache
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

		initCache();
	}

	public KVServer(int port, String serverName, String zkHost, int zkPort, String cacheStrategy, int cacheSize)
			throws InterruptedException, KeeperException {
		this.port = port;
		this.serverName = serverName;
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		this.zkHost = zkHost;
		this.zkPort = zkPort;

		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(cacheStrategy);

		initCache();
		initHeartbeat();
	}

	public KVServer(int port, int cacheSize, String algo, boolean test) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(algo);
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		initCache();

		if (test) {
			Thread testThread = new Thread(this);
			testThread.start();
		}
	}

	private void initCache() {
		if (this.cacheAlgo == CacheStrategy.None) {
			this.cache = null;
		} else { // allocate cache
			try {
				Constructor<?> constructorCache = Class.forName("cache." + this.cacheAlgo + "Cache")
						.getConstructor(Integer.class);
				this.cache = (Cache) constructorCache.newInstance(cacheSize);
			} catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
					| InvocationTargetException e) {
				logger.error(e);
			}
		}

	}

	private void initHeartbeat() {
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

	public void initKVServer(Metadata metadata) {
		// Initialize the KVServer with the metadata and block it for client requests,
		// i.e., all client requests are rejected with an SERVER_STOPPED error message;
		// ECS requests have to be processed.
		this.serverStatus = ServerStatus.CLOSED;
		update(metadata);
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
		return hashRing.get(getNamePortHost()) != null;
	}

	public Boolean getHashRange() {
		// Returns true if the node is no longer in the hashring, meaning it has been
		// removed

		if (inHashRing() == false) {
			// This detects that it's not in the hashring anymore, thus it needs to die
			this.hash = null;
			this.endHash = null;
			return true;
		}

		Collection<BigInteger> keys = hashRing.values();
		ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
		Collections.sort(orderedKeys);

		this.hash = hashRing.get(getNamePortHost());
		if (orderedKeys.size() == 1) {
			this.endHash = null;
			return false;
		}
		Integer nextIdx = orderedKeys.indexOf(this.hash);
		nextIdx = (nextIdx + 1) % orderedKeys.size();

		this.endHash = orderedKeys.get(nextIdx);
		return false;
	}

	public void moveData(Metadata metadata) {
		// Transfer a subset (range) of the KVServer's data to another KVServer
		// Send a notification to the ECS, if data transfer is completed.

		// Update internal metadata with the metadata is just recieved in the new
		// message
		update(metadata);

		Boolean die = getHashRange();
		lockWrite();

		ECSNode receiverNode = metadata.getReceiverNode();
		String hostOfReceiver = receiverNode.getNodeHost();
		int portOfReceiver = receiverNode.getNodePort();
		String nameOfReceiver = receiverNode.getNodeName();
		BigInteger hash = receiverNode.getHash();
		BigInteger endHash = receiverNode.getEndHash();

		if (nameOfReceiver == this.serverName) {
			unLockWrite();
			return;
		}

		try {
			// Get data from Persistant Storage to move to new server
			// If die = true, it will move all the data in its storage
			String dataInRange = persistantStorage.getDataInRange(hash, endHash, die);

			Socket socket = new Socket(hostOfReceiver, portOfReceiver);
			OutputStream output = socket.getOutputStream();

			JSONMessage json = new JSONMessage();
			json.setMessage(StatusType.PUT_MANY.name(), "put_many", dataInRange, null);
			byte[] jsonBytes = json.getJSONByte();

			output.write(jsonBytes, 0, jsonBytes.length);
			output.flush();
			output.close();
			socket.close();
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
		getHashRange();
	}

	public HashMap<String, BigInteger> getOrder() {
		return this.hashRing;
	}

	public boolean isMe(String toHash) {
		return utils.isKeyInRange(this.hash, this.endHash, toHash);
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		return serverSocket.getInetAddress().getHostName();
	}

	public String getNamePortHost() {
		String rval = this.serverName + ":" + getPort() + ":" + "127.0.0.1";
		return rval;
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
		if (this.cache != null) {
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
		if (this.cache != null) {
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
			if (args.length != 1 && args.length != 6) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else if (args.length == 6) {
				int port = Integer.parseInt(args[0]);
				String serverName = args[1];
				String zkHost = args[2];
				int zkPort = Integer.parseInt(args[3]);
				String cacheStrategy = args[4];
				int cacheSize = Integer.parseInt(args[5]);
				new KVServer(port, serverName, zkHost, zkPort, cacheStrategy, cacheSize).run();
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
