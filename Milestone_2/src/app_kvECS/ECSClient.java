package app_kvECS;

import java.util.Map;
import java.util.Collection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logging.ECSLogSetup;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;


import ecs.IECSNode;

public class ECSClient implements IECSClient, Runnable {

	private static Logger logger = Logger.getRootLogger();
    private String[] servers;
    private final String SERVER_JAR = "m2-server.jar";
    private final String CONFIG_FILEPATH = "./servers.cfg";

    // UI vars
    private boolean stop = false;
    private static final String PROMPT = "ECSAdmin> ";


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
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
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

        //TODO: launch storage server comprised of m servers
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
                    // add node
                    addNode("test",0);
                    break;
                case "addnodes":
                    System.out.println("Adding nodes");
                    // add node
                    addNodes(0,"test",0);
                    break;
                case "removenode":
                    System.out.println("Removing node");
                    //removeNodes();
                    String g;
                    break;
                case "help":
                    //TODO: write printHelp function
                    String g1;
                    // printHelp();
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
