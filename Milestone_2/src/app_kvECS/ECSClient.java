package app_kvECS;

import java.util.Map;
import java.util.Collection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logging.ECSLogSetup;
import shared.exceptions.UnexpectedFormatException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.lang.IllegalStateException;
import java.util.ArrayList;
import java.util.Iterator;

import ecs.ECSNode;
import ecs.IECSNode;
import ecs.ECSNode.NodeStatus;

public class ECSClient implements IECSClient, Runnable {

	private static Logger logger = Logger.getRootLogger();
    private String[] servers;
    private final String SERVER_JAR = "m2-server.jar";
    private final String CONFIG_FILEPATH = "./servers.cfg";
    private HashMap<String, ECSNode> allServerMap = new HashMap<String, ECSNode>();
    private HashMap<String, ECSNode> currServerMap = new HashMap<String, ECSNode>();

    // UI vars
    private boolean stop = false;
    private static final String PROMPT = "ECSAdmin> ";


    public ECSClient(){
        // load servers from config file
        getServerMap();
    }
    
    private void getServerMap(){
        try {
            BufferedReader file = new BufferedReader(new FileReader(CONFIG_FILEPATH));
            StringBuffer inputBuffer = new StringBuffer();
            String line;
            String keyFromFile;
            while ((line = file.readLine()) != null) {
                // Get info from each line: name, host, port
                String[] serverInfo = line.split(" ");

                if (serverInfo.length != 3){
                    logger.error("Error while reading config file: " + line);
                    throw new UnexpectedFormatException("Error while reading config file: " + line);
                }
                int port = Integer.parseInt(serverInfo[2]);
                // create ECSNode w/ status OFFLINE
                ECSNode serverNode = new ECSNode(serverInfo[0], port, serverInfo[1], ECSNode.NodeStatus.OFFLINE);
                // add to all server map <3 
                allServerMap.put(serverInfo[0], serverNode);
            }
        } catch (Exception e){
            logger.error("Could not read from file");
        }
    }


    @Override
    public boolean start() {
        /**
         * Starts the storage service by calling start() on all KVServer instances that participate in the service.\
         * @throws Exception    some meaningfull exception on failure
         * @return  true on success, false on failure
         */
        // for (String server : servers){
        //     // start server and attach it to zookeeper

        // }

        return false;
    }

    @Override
    public boolean stop() {
        // TODO something with zookeepr
        Iterator<Map.Entry< String,ECSNode> > it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry)it.next();
            ECSNode node = pair.getValue();
            node.setStatus(NodeStatus.STOPPED); 
            allServerMap.put(pair.getKey().toString(), node);
            // WILL UPDATING THE MAP WHILE ITERATING THROUGH IT MESS IT UP?
            currServerMap.put(pair.getKey().toString(), node);
        }

        return true;
    }

    @Override
    public boolean shutdown() {
        // TODO something with zookeepr
        Iterator<Map.Entry< String,ECSNode> > it = currServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry)it.next();
            ECSNode node = pair.getValue();
            node.setStatus(NodeStatus.OFFLINE);
            allServerMap.put(pair.getKey().toString(), node);
            // WILL UPDATING THE MAP WHILE ITERATING THROUGH IT MESS IT UP?
            currServerMap.put(pair.getKey().toString(), node);
        }

        currServerMap.clear();
        // System.exit(0);
        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        addNodes(1, cacheStrategy, cacheSize);
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        ArrayList<String> availServers = getAvailableServers();

        if (availServers.size() == 0) {
            logger.error("There are no more available servers");
            return null;
        } else if (count > availServers.size()) {
            logger.error("There are no not enough available servers");
            return null;
        }

        for (int i = 0; i < count; i++) {
            String serverName = availServers.remove(0); // Remove first element
            ECSNode serverNode = allServerMap.get(serverName);
            serverNode.setStatus(NodeStatus.STARTING);
            allServerMap.put(serverName, serverNode);
            currServerMap.put(serverName, serverNode);
        }
                    
        // TODO what am I returning???
        return null;
    }

    @Override
    public ArrayList<String> getAvailableServers() {
        ArrayList<String> availServers= new ArrayList<String>();

        Iterator<Map.Entry< String,ECSNode> > it = allServerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ECSNode> pair = (Map.Entry)it.next();
            ECSNode node = pair.getValue();
            NodeStatus status = node.getStatus();
            if (status == NodeStatus.OFFLINE) {
                availServers.add(pair.getKey().toString());
            }
        }

        return availServers;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
 
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        for (String name : nodeNames) {
            ECSNode serverNode = allServerMap.get(name);
            serverNode.setStatus(NodeStatus.OFFLINE);
            allServerMap.put(name, serverNode);
            currServerMap.remove(name);
        }
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.trim().split("\\s+");
        String action = tokens[0];
        try{
            switch(action){
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
                        ArrayList<String> nodeNames= new ArrayList<String>();
                        for (int i = 1; i < tokens.length; i++) {
                            nodeNames.add(tokens[i]);
                        }
                        removeNodes(nodeNames);
                    }
                    String g;
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
        // creating ECS UI
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
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
