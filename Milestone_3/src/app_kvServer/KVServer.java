package app_kvServer;

import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.OutputStream;
import java.io.IOException;
import java.net.BindException;
import java.util.*;
import java.math.BigInteger;
import java.util.concurrent.CyclicBarrier;
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
	public PersistantStorage persistantStorage;
	private Socket client;
	private boolean running;
	private String hostName;
	private ArrayList<Thread> threads;
	public ServerStatus serverStatus;
	private ServerSocket serverSocket;
	private Controller controller;

	private int replicateReceiverPort;
	private HashMap<String, Replicate> actingReplicates;

	private ECSConnection ecsConnection;
	public String serverName;
	private String zkHost;
	private int zkPort;

	private int zkTimeout = 1000;
	private ZooKeeper zk;
	private ZooKeeperApplication zkApp;

	// hashring variables
	public BigInteger hash;
	public BigInteger endHash;
	private HashMap<String, BigInteger> hashRing;

	private Utils utils = new Utils();

	// caching variables
	private int cacheSize;
	private CacheStrategy cacheAlgo;
	private Cache cache;

	private HashMap<String, String> logs = new HashMap<String, String>();

	public KVServer(int port, int cacheSize, String algo) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(algo);
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		this.actingReplicates = new HashMap<String, Replicate>();
		initCache();
	}

	public KVServer(int port, String serverName, String zkHost, int zkPort, String cacheStrategy, int cacheSize,
			int replicateReceiverPort)
			throws InterruptedException, KeeperException {
		this.port = port;
		this.serverName = serverName;
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		this.zkHost = zkHost;
		this.zkPort = zkPort;

		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(cacheStrategy);

		this.replicateReceiverPort = replicateReceiverPort;
		this.actingReplicates = new HashMap<String, Replicate>();

		initCache();
		// initHeartbeat();
	}

	public KVServer(int port, int cacheSize, String algo, boolean test) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.cacheAlgo = CacheStrategy.valueOf(algo);
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
		this.threads = new ArrayList<Thread>();

		this.actingReplicates = new HashMap<String, Replicate>();
		initCache();

		if (test) {
			Thread testThread = new Thread(this);
			testThread.start();
		}
	}

	private void initCache() {
		if (this.cacheAlgo == CacheStrategy.None) {
			this.cache = null;
		} else {
			// allocate cache
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

	public void addActingReplicate(Replicate replicate) {
		actingReplicates.put(replicate.getMasterNamePortHost(), replicate);
	}

	public void removeActingReplicate(Replicate replicate) {
		actingReplicates.remove(replicate.getMasterNamePortHost());
	}

	private void initHeartbeat() {
		zkApp = new ZooKeeperApplication(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, zkPort, zkHost);

		// Connect to ZK
		try {
			zk = zkApp.connect(zkHost + ":" + String.valueOf(zkPort), zkTimeout);
		} catch (InterruptedException | IOException e) {
			logger.error("Cannot connect to ZK server!", e);
			System.exit(1);
		}

		// Confirm if the zNode '/root/<server name>' exists. Exit if it doesn't,
		// because it means the ECS didn't successfully initialize it
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

		// Confirm if the zNode '/heartbeat/<server name>' exists. Exit if it doesn't,
		// because it means the ECS didn't successfully initialize it
		try {
			// Create en ephemeral heartbeat znode
			String heartbeatPath = ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH + "/" +
					serverName;
			zkApp.create(heartbeatPath, serverName + " heartbeat", CreateMode.EPHEMERAL);
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

	public void initKVServer(Metadata metadata) throws Exception {
		// Initialize the KVServer with the metadata and block it for client requests,
		this.controller = new Controller(this);

		stop(); // Need this for initial status for the server
		update(metadata);

		// begin updates every 2 minutes, starting 1 minute after init update
		updateReplicasAsync();

		if (inHashRing() && this.hashRing.size() == 1) {
			// Get old data from global storage if it's the first server being booted up
			persistantStorage.getFromGlobalStorage();
		}
	}

	public void updateReplicasAsync() {
		Timer timer = new Timer();
		TimerTask updateReplicas = new TimerTask() {
			@Override
			public void run() {
				if (controller != null) {
					controller.updateReplicates();
					logger.info("Replicas updated <3");
				} else {
					logger.error("Controller not successfully implemented for updates");
				}
			}
		};

		// update every 2 minutes (delay by 1 min before starting)
		timer.scheduleAtFixedRate(updateReplicas, 60000, 120000);
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
			// When inHashRing() is false, it means this server is not in the hashring
			// anymore, thus it has been removed
			this.hash = null;
			this.endHash = null;
			return true;
		}

		Collection<BigInteger> keys = hashRing.values();
		ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
		Collections.sort(orderedKeys);

		this.hash = hashRing.get(getNamePortHost());

		// If it's the only server in the hashring, set the end hash to null to indicate
		// that
		if (orderedKeys.size() == 1) {
			this.endHash = null;
			return false;
		}
		// Else find its endHash in the sorted hashring, which is the next server's
		// start hash
		Integer nextIdx = orderedKeys.indexOf(this.hash);
		nextIdx = (nextIdx + 1) % orderedKeys.size();

		this.endHash = orderedKeys.get(nextIdx);
		return false;
	}

	public void moveData(Metadata metadata) throws Exception {
		// Transfer a subset (range) of the KVServer's data to another KVServer

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

		if (nameOfReceiver.equals(this.serverName)) {
			// It's being told to move the data to itself
			if (die == true) {
				// This is the last server and it's being told to die
				// Move its storage to global_storage.txt
				persistantStorage.moveToGlobalStorage();
			}
			unLockWrite();
			try {
				TimeUnit.SECONDS.sleep(5);
				kill();
			} catch (Exception e) {
				logger.error("Unable to kill server");
			}
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

		// update replicas after keys are cut + server not dying
		logger.info("Updating replicas on movedata");
		controller.updateReplicasOnMoveData();
		logger.info("Updated replicas on movedata");
	}

	public void moveReplicateData(Metadata metadata) {
		// Transfer a subset (range) of the KVServer's data to another KVServer
		lockWrite();

		ECSNode receiverNode = metadata.getReceiverNode();
		String hostOfReceiver = receiverNode.getNodeHost();
		// needs to be REPLICATE RECEIVER PORT
		int portOfReceiver = receiverNode.getNodePort();
		String nameOfReceiver = receiverNode.getNodeName();

		nameOfReceiver = nameOfReceiver.split("@")[0];
		String deadNamePortHost = nameOfReceiver.split("@")[1];

		if (deadNamePortHost.split(":")[0].equals(this.serverName)) {
			// It's being told to move the data to itself
			unLockWrite();
			return;
		}

		String replicateData = "";
		try {
			// get relevant replica
			for (Replicate recoveryReplica : actingReplicates.values()) {
				if (recoveryReplica.getMasterNamePortHost() == deadNamePortHost) {
					// get the data
					replicateData = recoveryReplica.getAllReplicateData();
				}
			}

			// Figure out where to send to
			Socket socket = new Socket(hostOfReceiver, portOfReceiver);
			OutputStream output = socket.getOutputStream();

			JSONMessage json = new JSONMessage();
			json.setMessage(StatusType.PUT_MANY.name(), "put_many", replicateData, null);
			byte[] jsonBytes = json.getJSONByte();

			output.write(jsonBytes, 0, jsonBytes.length);
			output.flush();
			output.close();
			socket.close();
		} catch (Exception e) {
			logger.error("Unable to send recovery replicate data to node " + nameOfReceiver + ", " + e);
		}

		unLockWrite();
	}

	public void update(Metadata metadata) {
		// Update the metadata repository of this server
		this.hashRing = metadata.order;
		getHashRange();

		// update replica
		controller.setReplicationServers(metadata.order, metadata.replicateReceiverPorts);
	}

	public HashMap<String, BigInteger> getOrder() {
		return this.hashRing;
	}

	public boolean isMe(String toHash) {
		return utils.isKeyInRange(this.hash, this.endHash, toHash);
	}

	public String keyInReplicasRange(String key) {
		// Determine if the hash of key falls in the hash ring of the replicas this
		// server maintains
		// Returns name of the replica, else null

		ArrayList<BigInteger> orderedKeys = new ArrayList<>(hashRing.values());
		Collections.sort(orderedKeys);

		// For each replicant, determine its hash range and if the key falls in it
		// actingReplicates
		for (Replicate currReplicate : actingReplicates.values()) {
			String masterNamePortHost = currReplicate.getMasterNamePortHost();

			BigInteger hash = hashRing.get(masterNamePortHost);
			Integer i = orderedKeys.indexOf(hash);
			// if (i == orderedKeys.size() - 1) {
			// i = 0;
			// } else {
			// i++;
			// }
			i = (i + 1) % orderedKeys.size();
			BigInteger endHash = orderedKeys.get(i);

			if (utils.isKeyInRange(hash, endHash, key)) {
				return masterNamePortHost;
			}
		}
		return null;
	}

	public String getKVFromReplica(String key, String masterNamePortHost) throws Exception {
		for (Replicate replica : actingReplicates.values()) {
			if (replica.getMasterNamePortHost() == masterNamePortHost) {
				String value = replica.getKVFromReplica(key);
				if (value == null) {
					logger.warn("Key " + key + " is not found in replica " + masterNamePortHost);
					throw new Exception("Key is not found in replica");
				}
				return value;
			}
		}
		return null;
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public String getHostname() {
		// "127.0.0.1" is harded coded and required due to legacy code, however, ut
		// don't actually use the host for anything important
		return "127.0.0.1";
		// return serverSocket.getInetAddress().getHostName();
	}

	public String getNamePortHost() {
		return this.serverName + ":" + getPort() + ":" + getHostname();
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

		StatusType putStatus = this.persistantStorage.put(key, value);
		// Add this command to the logs, to be sent to the replicates

		if (putStatus == StatusType.PUT_SUCCESS || putStatus == StatusType.DELETE_SUCCESS
				|| putStatus == StatusType.PUT_UPDATE) {
			addToLogs(key, value);
			controller.updateReplicates();
		}
		return putStatus;
	}

	private void addToLogs(String key, String value) {
		this.logs.put(key, value);
	}

	public String getStringLogs(boolean clearLogs) {
		StringBuffer buffer = new StringBuffer();

		for (Map.Entry<String, String> entry : this.logs.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();

			JSONMessage log = new JSONMessage();
			log.setMessage("PUT", key, value);
			buffer.append(log.serialize(false) + "\n");
		}

		if (clearLogs) {
			this.logs.clear();
		}

		return buffer.toString();
	}

	@Override
	public void clearCache() {
		if (this.cache != null) {
			this.cache.clear();
		}
		logger.info("Cache cleared");
	}

	public String getAllFromStorage() {
		return this.persistantStorage.getAllFromStorage();
	}

	public StatusType appendToStorage(String keyValues) throws Exception {
		return this.persistantStorage.appendToStorage(keyValues);
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

	private void initializeReplicateListener() {
		try {
			ServerSocket replicateReceiveSocket = new ServerSocket(replicateReceiverPort);
			new Thread(new ReplicateServer(replicateReceiveSocket, this)).start();
			logger.info("Replicate server listening on port: " + replicateReceiveSocket.getLocalPort());
		} catch (IOException e) {
			logger.error("Could not open replicate listening socket");
			e.printStackTrace();
		} catch (Exception e_1) {
			logger.error("Error creating thread: " + e_1);
		}
	}

	@Override
	public void run() {
		running = initializeServer();

		initializeReplicateListener();

		if (serverSocket != null) {
			while (isRunning()) {
				try {
					client = serverSocket.accept();
					ServerConnection serverConnection = new ServerConnection(client, this);
					Thread newThread = new Thread(serverConnection);
					newThread.start();
					this.threads.add(newThread);

					logger.info(
							"Connected to KVServer (client port " + port + ") " + client.getInetAddress().getHostName()
									+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! Unable to establish connection to server. \n", e);
				} catch (Exception e) {
					logger.error("Error! \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	public boolean isRunning() {
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
			if (args.length != 1 && args.length != 7) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else if (args.length == 7) {
				int port = Integer.parseInt(args[0]);
				String serverName = args[1];
				String zkHost = args[2];
				int zkPort = Integer.parseInt(args[3]);
				String cacheStrategy = args[4];
				int cacheSize = Integer.parseInt(args[5]);
				int replicatePort = Integer.parseInt(args[6]);
				new KVServer(port, serverName, zkHost, zkPort, cacheStrategy, cacheSize, replicatePort).run();
			} else {
				int port = Integer.parseInt(args[0]);
				new KVServer(port, 10, "FIFO").run();
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
