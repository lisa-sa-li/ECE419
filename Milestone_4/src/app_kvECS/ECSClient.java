package app_kvECS;

import java.util.Map;
import java.util.Collection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logging.ECSLogSetup;
import shared.exceptions.UnexpectedFormatException;
import java.util.concurrent.TimeUnit;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.lang.IllegalStateException;
import java.lang.Runtime;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNode.NodeStatus;
import ecs.ZooKeeperApplication;
import ecs.HashRing;

public class ECSClient implements IECSClient, Runnable {

    private static Logger logger = Logger.getRootLogger();
    private String[] servers;
    private String SERVER_JAR = "m2-server.jar";
    private static String CONFIG_FILEPATH = "./servers.cfg";

    private HashMap<String, ECSNode> allServerMap = new HashMap<String, ECSNode>();
    private HashMap<String, ECSNode> currServerMap = new HashMap<String, ECSNode>();
    private HashMap<String, String> serverInfo = new HashMap<String, String>();
    private HashRing hashRing;

    private int zkPort = 2181;
    private String zkHost = "127.0.0.1";
    private int zkTimeout = 1000;
    private ZooKeeper zk;
    private ZooKeeperApplication zkApp;

    private ServerSocket ecsServerSocket;
    private String hostname = "127.0.0.1";
    private ArrayList<Thread> threads;

    // UI vars
    private boolean stop = false;
    private static final String PROMPT = "ECSAdmin> ";

    private Random rand = new Random();

    public ECSClient(String configFile) {
        // Load servers from config file
        getServerMap(configFile);

        // Initialize hash ring
        hashRing = new HashRing(this.serverInfo);

        // Connect to ZooKeeper
        zkApp = new ZooKeeperApplication(ZooKeeperApplication.ZK_NODE_ROOT_PATH, zkPort, zkHost);
        try {
            zk = zkApp.connect(zkHost + ":" + String.valueOf(zkPort), zkTimeout);
        } catch (InterruptedException | IOException e) {
            logger.error("Cannot connect to ZK server!", e);
        }

        // Create a "/root" znode
        try {
            if (zk.exists(ZooKeeperApplication.ZK_NODE_ROOT_PATH, false) == null) {
                zkApp.create(ZooKeeperApplication.ZK_NODE_ROOT_PATH, "root_node");
            }
            if (zk.exists(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, false) == null) {
                zkApp.create(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, "heartbeat_node");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot create root or heartbeat paths in ZK! " + e);
        }
    }

    public void newConnection(ECSNode node) throws Exception {
        // Create a new ECSConnection used by a ECSNode to send and recieve messages
        // from its respective server
        try {
            int port = node.getNodePort();
            String serverName = node.getNodeName();

            Socket clientSocket = new Socket(this.hostname, port);
            ECSConnection ecsConnection = new ECSConnection(clientSocket);
            // set socket in ecsConnection
            node.setConnection(ecsConnection);

            logger.info("Server " + serverName + " connected to " + clientSocket.getInetAddress().getHostName()
                    + " on port " + clientSocket.getPort());
        } catch (IOException e) {
            logger.error("ERROR MAKING NEW CONNECTION IN ECSCLIENT");
            throw e;
        }
    }

    private void getServerMap(String configPath) {
        try {
            BufferedReader file = new BufferedReader(new FileReader(configPath));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            while ((line = file.readLine()) != null) {
                // Get info from each line: name, host, port
                String[] serverInfo = line.split(" ");

                if (serverInfo.length != 3) {
                    logger.error("Error while reading config file: " + line);
                    throw new UnexpectedFormatException("Error while reading config file: " + line);
                }
                int port = Integer.parseInt(serverInfo[2]);
                // create ECSNode w/ status OFFLINE
                ECSNode serverNode = new ECSNode(serverInfo[0], port, serverInfo[1], ECSNode.NodeStatus.OFFLINE);
                // add to all server map <3
                allServerMap.put(serverInfo[0], serverNode);
                this.serverInfo.put(serverInfo[0], serverInfo[2] + ":" + serverInfo[1]);
            }
        } catch (Exception e) {
            logger.error("Could not read from file");
        }
    }

    public ArrayList<Integer> getCurrentPorts() {
        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        ArrayList<Integer> portNumbersCurrent = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            ECSNode node = pair.getValue();
            int port = node.getNodePort();
            portNumbersCurrent.add(port);
        }
        return portNumbersCurrent;
    }

    public ArrayList<String> getCurrentServers() {
        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        ArrayList<String> namesCurrent = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            ECSNode node = pair.getValue();
            String name = node.getNodeName();
            namesCurrent.add(name);
        }
        return namesCurrent;
    }

    @Override
    public boolean start() {
        boolean startSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            // String zNodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;
            // try {
            // zkApp.createOrSetData(zNodePath, name);
            // } catch (KeeperException | InterruptedException e) {
            // startSuccess = false;
            // continue;
            // } catch (Exception e) {
            // startSuccess = false;
            // logger.error(e);
            // continue;
            // }

            node.setStatus(NodeStatus.STARTING);
            allServerMap.put(name, node);
            currServerMap.put(name, node);
        }
        // This sends a START message to the servers
        hashRing.startAll();
        return startSuccess;
    }

    @Override
    public boolean stop() {
        boolean stopSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            String zNodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;
            try {
                zkApp.createOrSetData(zNodePath, name);
            } catch (KeeperException | InterruptedException e) {
                stopSuccess = false;
                logger.error("Cannot stop ZK " + e);
                continue;
            } catch (Exception e) {
                stopSuccess = false;
                logger.error(e);
                continue;
            }

            node.setStatus(NodeStatus.STOPPED);
            allServerMap.put(name, node);
            currServerMap.put(name, node);
        }
        // This sends a STOP message to the servers
        hashRing.stopAll();
        return stopSuccess;
    }

    @Override
    public boolean shutdown() {
        boolean shutdownSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            final String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            final CountDownLatch countDownLatch = new CountDownLatch(1);

            String heartbeatPath = ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH + "/" + name;
            try {
                zk.exists(heartbeatPath, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event == null) {
                            return;
                        }
                        if (EventType.NodeDeleted == event.getType()) {
                            logger.info("HEARTBEAT DELETED: Server " + name + " has been closed");
                        }
                        countDownLatch.countDown();
                    }
                });
            } catch (KeeperException | InterruptedException e) {
                shutdownSuccess = shutdownSuccess & false;
                logger.error("Cannot detect shutdown of server " + name + "by checking its heartbeat", e);
                continue;
            }

            try {
                boolean complete = countDownLatch.await(2L, TimeUnit.SECONDS);
                shutdownSuccess = shutdownSuccess & complete;

                node.setStatus(NodeStatus.OFFLINE);
                allServerMap.put(name, node);
                currServerMap.put(name, node);
                // This sends a SHUTDOWN message to the server
                hashRing.removeNode(name);
            } catch (Exception e) {
                shutdownSuccess = shutdownSuccess & false;
                logger.error("Cannot detect shutdown of server " + name + "by checking its heartbeat", e);
            }
        }

        currServerMap.clear();
        return shutdownSuccess;
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        addNodes(1, cacheStrategy, cacheSize);
        return null;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        ArrayList<String> availServers = getAvailableServers();

        if (availServers.size() == 0) {
            logger.error("There are no more available servers");
            return null;
        } else if (count > availServers.size()) {
            logger.error("There are not enough available servers");
            return null;
        }

        ArrayList<ECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        ArrayList<ECSNode> nodesAdded = new ArrayList<ECSNode>();

        for (ECSNode node : nodes) {
            String serverName = node.getNodeName();
            node.setStatus(NodeStatus.STARTING); // Not sure about the status
            node.setCacheInfo(cacheSize, cacheStrategy);
            allServerMap.put(serverName, node);
            currServerMap.put(serverName, node);

            // Start the KVServer by issuing an SSH call to the machine
            // + System.getProperty("user.dir") + "/"
            String cmd = "java -jar " + SERVER_JAR + " "
                    + String.valueOf(node.getNodePort()) + " " + serverName + " " + zkHost + " "
                    + String.valueOf(zkPort) + " " + cacheStrategy + " "
                    + String.valueOf(cacheSize);

            if (!node.getNodeHost().equals("127.0.0.1") && !node.getNodeHost().equals("localhost")) {
                cmd = "ssh -n " + node.getNodeHost() + " nohup " + cmd + " &";
            }

            logger.debug("This is the command: " + cmd);
            try {
                Process p = Runtime.getRuntime().exec(cmd);
                boolean completed = awaitNode(serverName);

                newConnection(node);
                hashRing.addNode(node);
                nodesAdded.add(node);
            } catch (Exception e) {
                logger.error("Cannot start the server through an SSH call", e);
            }
        }
        return nodesAdded;
    }

    @Override
    public ArrayList<String> getAvailableServers() {
        ArrayList<String> availServers = new ArrayList<String>();

        Iterator<Map.Entry<String, ECSNode>> it = allServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            ECSNode node = pair.getValue();
            NodeStatus status = node.getStatus();
            if (status == NodeStatus.OFFLINE || status == NodeStatus.STOPPED) {
                availServers.add(pair.getKey().toString());
            }
        }

        return availServers;
    }

    @Override
    public ArrayList<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > allServerMap.size()) {
            logger.error("There are not enough servers");
            return null;
        }

        ArrayList<String> availServers = getAvailableServers();
        ArrayList<ECSNode> nodes = new ArrayList<ECSNode>();

        for (int i = 0; i < count; i++) {
            // Choose a random server, also remove it from availServers, so it can't be used
            // again in this loop
            int int_random = rand.nextInt(availServers.size());
            String serverName = availServers.remove(int_random);
            ECSNode node = allServerMap.get(serverName);
            node.setCacheInfo(cacheSize, cacheStrategy);
            String znodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + serverName;
            try {
                zkApp.createOrSetData(znodePath, serverName);
            } catch (KeeperException | InterruptedException e) {
                logger.error(e);
            }

            nodes.add(node);
        }

        return nodes;
    }

    @Override
    public boolean awaitNode(final String name) throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        String heartbeatPath = ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH + "/" + name;
        try {
            // Set watcher on the heartbeat znode being created
            zk.exists(heartbeatPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event == null) {
                        return;
                    }
                    if (EventType.NodeCreated == event.getType()) {
                        logger.info("HEARTBEAT DETECTED: Server " + name + " has been started");
                    }
                    countDownLatch.countDown();
                }
            });
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot detect the start of server " + name + "by checking its heartbeat", e);
            return false;
        }

        try {
            TimeUnit.SECONDS.sleep(1);
            return countDownLatch.await(2L, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Cannot detect the start of server " + name + "by checking its heartbeat", e);
            return false;
        }
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        if (currServerMap.size() == 1) {
            logger.error("You may not remove the last running node: there must be at least one active server.");
            return false;
        } else if (nodeNames.size() >= currServerMap.size()) {
            logger.error("You are removing too many nodes. There must be at least one active server.");
            return false;
        }

        for (String name : nodeNames) {
            ECSNode serverNode = allServerMap.get(name);
            serverNode.setStatus(NodeStatus.OFFLINE);
            allServerMap.put(name, serverNode);
            currServerMap.remove(name);
            hashRing.removeNode(name);
        }
        return true;
    }

    @Override
    public HashMap<String, ECSNode> getNodes() {
        return hashRing.getHashRingMap();
    }

    @Override
    public ECSNode getNodeByKey(String Key) {
        return getNodes().get(Key);
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.trim().split("\\s+");
        String action = tokens[0];

        try {
            switch (action) {
                case "start":
                    System.out.println("Launching all storage servers");
                    start();
                    break;
                case "stop":
                    System.out.println("Stopping all storage servers");
                    stop();
                    break;
                case "addnode":
                    System.out.println("Adding node");
                    addNode(tokens[1], Integer.parseInt(tokens[2]));
                    break;
                case "addnodes":
                    if (tokens.length != 4) {
                        logger.error("Invalid number of parameters! Missing number of nodes to add");
                    } else {
                        System.out.println("Adding nodes");
                        int count = Integer.parseInt(tokens[1]);
                        addNodes(count, tokens[2], Integer.parseInt(tokens[3]));
                    }
                    break;
                case "removenode":
                    if (tokens.length < 2) {
                        logger.error("Invalid number of parameters! Missing names of node(s) to remove");
                    } else {
                        System.out.println("Removing node(s)");
                        ArrayList<String> nodeNames = new ArrayList<String>();
                        for (int i = 1; i < tokens.length; i++) {
                            nodeNames.add(tokens[i]);
                        }
                        removeNodes(nodeNames);
                    }
                    break;
                case "help":
                    printHelp();
                    break;
                case "shutdown":
                    System.out.println("Shutting down");
                    shutdown();
                    break;
                default:
                    logger.error("Unknown command.");
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unknown Error: " + e.getMessage());
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(PROMPT).append("ECSCLIENT COMMANDS:\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("start");
        sb.append(PROMPT).append("stop");
        sb.append(PROMPT).append("shutdown");
        sb.append(PROMPT).append("addnode");
        sb.append(PROMPT).append("addnodes <num_nodes>");
        sb.append(PROMPT).append("removenode <server name> <server name> ...");
        sb.append(PROMPT).append("help");
        sb.append(PROMPT).append("quit");
        sb.append("\t\t\t Exits the program \n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append(":::::::::::::::::::::::::::::::: \n");
        System.out.println(sb.toString());
    }

    public void run() {
        while (!stop) {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated.");
                logger.error("CLI does not respond - Application terminated.");
            }
        }
    }

    public static void main(String[] args) {
        try {
            new ECSLogSetup("logs/ecs.log", Level.ALL);
            ECSClient ecsClient = new ECSClient(CONFIG_FILEPATH);
            ecsClient.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
