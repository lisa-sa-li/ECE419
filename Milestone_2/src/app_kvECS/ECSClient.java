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
import java.io.BufferedReader;
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
    private String CONFIG_FILEPATH = "./servers.cfg";
    // private String ZooKeeperApplication.ZK_NODE_ROOT_PATH = "./BLAH";

    private HashMap<String, ECSNode> allServerMap = new HashMap<String, ECSNode>();
    private HashMap<String, ECSNode> currServerMap = new HashMap<String, ECSNode>();
    private HashRing hashRing;

    private int zkPort = 2181;
    private String zkHost = "127.0.0.1";
    private int zkTimeout = 1000;
    private ZooKeeper zk;
    private ZooKeeperApplication zkApp;

    private ServerSocket ecsServerSocket;
    private String hostname = "127.0.0.1";
    private ArrayList<Thread> threads;

    // To start ZooKeeper server: $ ./zkServer.sh start
    // To connect a client to the server: $ ​​./zkCli.sh -server 127.0.0.1:2181 *
    // Make sure that the port matches that in your zoo.cfg file (2181 is usually
    // the default in the cfg file, thus used here).

    // UI vars
    private boolean stop = false;
    private static final String PROMPT = "ECSAdmin> ";

    private Random rand = new Random();

    public ECSClient() {
        // load servers from config file
        getServerMap();

        hashRing = new HashRing();
        zkApp = new ZooKeeperApplication(ZooKeeperApplication.ZK_NODE_ROOT_PATH, zkPort, zkHost);
        try {
            zk = zkApp.connect(zkHost + ":" + String.valueOf(zkPort), zkTimeout);
        } catch (InterruptedException | IOException e) {
            logger.error("Cannot connect to ZK server!", e);
        }

        try {
            if (zk.exists(ZooKeeperApplication.ZK_NODE_ROOT_PATH, false) == null) {
                zkApp.create(ZooKeeperApplication.ZK_NODE_ROOT_PATH, "root_node");
            }
			if (zk.exists(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, false) == null) {
                zkApp.create(ZooKeeperApplication.ZK_HEARTBEAT_ROOT_PATH, "heartbeat");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot create root or heartbeat paths in ZK! " + e);
        } catch (Exception e) {
            logger.error("Hi2 " + e);
        }
    }

    public void newConnection(ECSNode node) throws Exception {
        try {
            System.out.println("NEW CONNECTION");
            int port = node.getNodePort();
            String serverName = node.getNodeName();
            System.out.println("NEW CONNECTION " + port + serverName);

            Socket clientSocket = new Socket(this.hostname, port);
            System.out.println("NEW CONNECTION3");
            ECSConnection ecsConnection = new ECSConnection(clientSocket, this);
            System.out.println("NEW CONNECTION4");
            node.setConnection(ecsConnection);
            System.out.println("NEW CONNECTION5");

            Thread newThread = new Thread(ecsConnection, serverName);
            System.out.println("NEW CONNECTION5.5");

            newThread.start();
            System.out.println("NEW CONNECTION5.9");

            this.threads.add(newThread);

            logger.info("Connected to " + clientSocket.getInetAddress().getHostName() + " on port "
                    + clientSocket.getPort());
            
            System.out.println("NEW CONNECTION6");
        } catch (IOException e) {
            throw e;
        }
    }

    private void getServerMap() {
        try {
            BufferedReader file = new BufferedReader(new FileReader(CONFIG_FILEPATH));
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
            }
        } catch (Exception e) {
            logger.error("Could not read from file");
        }
    }

    @Override
    public boolean start() {
        /**
         * Starts the storage service by calling start() on all KVServer instances that
         * participate in the service.\
         * 
         * @throws Exception some meaningfull exception on failure
         * @return true on success, false on failure
         */
        boolean startSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            String zNodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;
            try {
                zkApp.createOrSetData(zNodePath, "Some Start message tbd");
            } catch (KeeperException | InterruptedException e) {
                startSuccess = false;
                logger.error("Hi3" + e);
                continue;
            } catch (Exception e) {
                startSuccess = false;
                logger.error(e);
                continue;
            }

            node.setStatus(NodeStatus.STARTING);
            allServerMap.put(name, node);
            // WILL UPDATING THE MAP WHILE ITERATING THROUGH IT MESS IT UP?
            currServerMap.put(name, node);
        }

        return startSuccess;
    }

    @Override
    public boolean stop() {
        /**
         * Stops the service; all participating KVServers are stopped for processing
         * client
         * requests but the processes remain running.
         * 
         * @throws Exception some meaningfull exception on failure
         * @return true on success, false on failure
         */
        boolean stopSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            String zNodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;
            try {
                zkApp.createOrSetData(zNodePath, "Some Stop message tbd");
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
            // WILL UPDATING THE MAP WHILE ITERATING THROUGH IT MESS IT UP?
            currServerMap.put(name, node);
        }

        return stopSuccess;
    }

    @Override
    public boolean shutdown() {
        boolean shutdownSuccess = true;

        Iterator<Map.Entry<String, ECSNode>> it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();

            String zNodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;
            try {
                zkApp.createOrSetData(zNodePath, "Some offline message tbd");
            } catch (KeeperException | InterruptedException e) {
                shutdownSuccess = false;
                logger.error("Cannont shutdown ZK " + e);
                continue;
            } catch (Exception e) {
                shutdownSuccess = false;
                logger.error(e);
                continue;
            }

            node.setStatus(NodeStatus.OFFLINE);
            allServerMap.put(name, node);
            // WILL UPDATING THE MAP WHILE ITERATING THROUGH IT MESS IT UP? YES, BUT THEY
            // MAKE ITERATORS FOR THIS SPECIFIC CASE
            currServerMap.put(name, node);
            hashRing.removeNode(name);
        }

        currServerMap.clear();
        // System.exit(0);
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

        if (hashRing.isEmpty()) {
            try {
                System.out.println("EMPTY, go here:");
                hashRing.createHashRing(currServerMap);
                System.out.println("FINISHED HASHRING");
            } catch (Exception e){
                logger.error("Unable to initialize hashring");
                return null;
            }
        }

        ArrayList<ECSNode> nameArr = new ArrayList<ECSNode>();

        for (int i = 0; i < count; i++) {
            // Choose a random server, also remove it from availServers so it can't be used
            // again in this loop
            int int_random = rand.nextInt(availServers.size());
            String serverName = availServers.remove(int_random);
            ECSNode node = allServerMap.get(serverName);

            node.setStatus(NodeStatus.STARTING); // Not sure about the status
            allServerMap.put(serverName, node);
            currServerMap.put(serverName, node);
            nameArr.add(node);
            hashRing.addNode(node);
            

            // String javaCmd = String.join(" ",
            //         "java -jar",
            //         SERVER_JAR,
            //         String.valueOf(node.getNodePort()),
            //         node.getNodeName(),
            //         zkHost,
            //         String.valueOf(zkPort));

            // boolean isLocal = node.getNodeHost().equals("127.0.0.1") || node.getNodeHost().equals("localhost");

            // String cmd;

            // if (isLocal) {
            //   String cmd = javaCmd;
            // } else {
            //    String cmd = String.join(" ",
            //             "ssh -o StrictHostKeyChecking=no -n",
            //             node.getNodeHost(),
            //             "nohup",
            //             javaCmd,
            //             "&");
            // }


            // Start the KVServer by issuing an SSH call to the machine
            System.out.print("System.getProperty(user.dir)" + System.getProperty("user.dir"));
            String cmd = "java -jar " + System.getProperty("user.dir")+ "/" +  SERVER_JAR + " " + String.valueOf(node.getNodePort());
            // // 
            // String cmd2 = "ssh -n " + this.hostname + " nohup " + cmd + " ERROR &";
            System.out.println("THIS IS THE CMD STRING: " + cmd);
            try {
                Process p = Runtime.getRuntime().exec(cmd);
                // p.waitFor();
                // create new connection :*
                // MAKE SURE THIS HAPPENS AFTER ABOVE CALL - MAYBE DELAY NEEDED?
                TimeUnit.SECONDS.sleep(10);

                newConnection(node);
            } catch (Exception e) {
                System.out.println("THIS IS SSH error: " + e);

                logger.error("Cannot start the server through an SSH call", e);
            }
        }
        /*
         * Randomly choose <numberOfNodes> servers from the available machines and start
         * the KVServer by issuing an SSH call to the respective machine.
         * This call launches the storage server with the specified cache size and
         * replacement strategy. For simplicity, locate the KVServer.jar in the same
         * directory
         * as the ECS. All storage servers are initialized with the metadata and any
         * persisted data,
         * and remain in state stopped.
         * NOTE: Must call setupNodes before the SSH calls to start the servers and must
         * call awaitNodes before returning
         * 
         * @return set of strings containing the names of the nodes
         */
        return nameArr;
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
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        /**
         * Sets up `count` servers with the ECS (in this case Zookeeper)
         * 
         * @return array of strings, containing unique names of servers
         */

        if (count > allServerMap.size()) {
            logger.error("There are not enough servers");
            return null;
        }

        ArrayList<String> nodes = new ArrayList<String>();

        Iterator<Map.Entry<String, ECSNode>> it = allServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry) it.next();
            String name = pair.getKey().toString();
            ECSNode node = pair.getValue();
            NodeStatus status = node.getStatus();

            if (status == NodeStatus.OFFLINE) { // NOT SURE
                continue;
            }

            String znodePath = ZooKeeperApplication.ZK_NODE_ROOT_PATH + "/" + name;

            try {
                zkApp.createOrSetData(znodePath, "UNSURE WHAT MESSAGE TO SEND");
                nodes.add(name);
            } catch (KeeperException | InterruptedException e) {
                logger.error(e);
            }
        }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        /**
         * Wait for all nodes to report status or until timeout expires
         * 
         * @param count   number of nodes to wait for
         * @param timeout the timeout in milliseconds
         * @return true if all nodes reported successfully, false otherwise
         */

        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        if (nodeNames.size() > currServerMap.size()) {
            logger.error("You are removing too many nodes. There must be at least one active server.");
            return false;
        }
        // Error check, make sure there is at least one active node
        // TODO
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
                    addNode("None", 0);
                    break;
                case "addnodes":
                    if (tokens.length > 3) {
                        logger.error("Invalid number of parameters! Missing number of nodes to add");
                    } else {
                        System.out.println("Adding nodes");
                        int count = Integer.parseInt(tokens[1]);
                        addNodes(count, "None", 0);
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
            }
        } catch (Exception e) {
            logger.error("Unkown Error: " + e.getMessage());
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
        sb.append(PROMPT).append("removenode <?????>");
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
            ECSClient ecsClient = new ECSClient();
            ecsClient.run();
        }
        catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } 
        // catch (Exception e) {
        //     System.out.println("Error! Unable to initialize logger!");
        //     e.printStackTrace();
        //     System.exit(1);
        // }
    }
}
