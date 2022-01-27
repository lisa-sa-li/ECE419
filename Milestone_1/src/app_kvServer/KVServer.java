package app_kvServer;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.net.BindException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import shared.messages.TextMessage;
import logger.LogSetup;
import app_kvServer.PersistantStorage;

public class KVServer extends Thread implements IKVServer {

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private int cacheSize;
	private PersistantStorage persistantStorage;
	private ServerSocket serverSocket;
	private boolean running;
	private String hostName;

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) throws Exception {
		// TODO Auto-generated method stub
		this.port = port;
		this.cacheSize = cacheSize;
		this.persistantStorage = new PersistantStorage(String.valueOf(this.port));
	}
	
	@Override
	public int getPort(){
		return this.port;
	}

	@Override
    public String getHostname(){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		// TODO Auto-generated method stub
		return IKVServer.CacheStrategy.None;
	}

	@Override
    public int getCacheSize(){
		return this.cacheSize;
	}

	@Override
    public boolean inStorage(String key) throws Exception {
		return this.persistantStorage.inStorage(key);
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public String getKV(String key) throws Exception{
		String value = this.persistantStorage.get(key);
		if (value == null) {
			logger.warn(key + " is not found");
			throw new Exception("Key is not found");
		}
		return value;
	}

	@Override
    public void putKV(String key, String value) throws Exception{
		boolean success = this.persistantStorage.put(key, value);
		if (success == false) {
			logger.warn("Unable to put " + key + ": " + value + ")");
			throw new Exception("Unable to put (" + key + ": " + value + ")");
		}
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public void clearStorage(){
		this.persistantStorage.clearStorage();
	}

	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

	@Override
    public void run(){
		// TODO Auto-generated method stub
		running = initializeServer();

		if(serverSocket != null){
			while(isRunning()){
				try{
					Socket client = serverSocket.accept();
					ClientConnection connection = new ClientConnection(client);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+ " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " + "Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning() {
        return this.running;
    }

	@Override
    public void kill(){
	}

	@Override
    public void close(){
		running = false;
		try {
			serverSocket.close();
		} catch (IOException e){
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}

	public static void main(String[] args) {
		try {
			// TODO: param for log level
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 1){
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				// TODO: set params for cache size and strategy
				new KVServer(port, 1, "").start();
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
