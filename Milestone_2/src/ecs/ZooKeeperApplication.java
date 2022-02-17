package ecs;

import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

// All this code is from Sofia's tutorial

public class ZooKeeperApplication implements Watcher {
    private BufferedReader stdin;
    private boolean running = true;
    private static final String PROMPT = "ZKDemo> ";
    private static Logger logger = Logger.getRootLogger();
    private static Application demo;
    private ZooKeeper _zooKeeper = null;
    private String _rootZnode = "/rootZnode";

    // ****************************************************************
    // CLI
    // ***************************************************************
    public void run() {
        while (running) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                printError("CLI does not respond - Application terminated");
            }
        }
    }

    private void printError(String error) {

        System.out.println(PROMPT + "Error! " + error);
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        // INPUT: connect
        if (tokens[0].equals("connect")) {
            try {
                demo.connect();
            } catch (Exception e) {
                logger.info("Could not connect to ZK.");
            }
            try {
                demo.create(_rootZnode);
            } catch (Exception e) {
                logger.info("Could not create new Znode.");
            }
        }
        // INPUT: add <server name>
        else if (tokens[0].equals("add")) {
            logger.info("Adding new Znode, " + tokens[1]);
            try {
                demo.create(_rootZnode + tokens[1]);
            } catch (Exception e) {
                logger.info(e);
            }
        }
        // INPUT: getservers
        else if (tokens[0].equals("getservers")) {
            try {
                demo.getWorkers();
            } catch (Exception e) {
                logger.info(e);
            }
        }
        // INPUT: <unknown command>
        else {
            printError("Unknown command");

        }
    }

    // ****************************************************************
    // ZooKeeper Interaction
    // ***************************************************************
    // CLIENT CONNECTION
    public ZooKeeper connect(String host, int timeout) throws IOException, InterruptedException {
        _zooKeeper = new ZooKeeper(host, timeout, this);
        return _zooKeeper;
    }

    // CREATE A ZNODE
    public void create(String path) throws KeeperException, InterruptedException {
        byte[] data = "id=0, hash-start=0000, hash-end=FFFF".getBytes();
        _zooKeeper.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    // RETURN CHILDREN ZNODES
    void getWorkers() throws KeeperException,
            InterruptedException {
        List<String> servers = _zooKeeper.getChildren(_rootZnode, true);
        logger.info("Servers currently registered to " + _rootZnode);
        for (String temp : servers) {
            logger.info("server: " + temp);
        }
    }

    // PROCESS WATCHER NOTIFICATION
    @Override
    public void process(WatchedEvent event) {
        logger.info("WATCHER NOTIFICATION!");
        if (event == null) {
            return;
        }
        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();
        logger.info("Connection status:\t" + keeperState.toString());
        logger.info("Event type:\t" + eventType.toString());
        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                logger.info("Successfully connected to ZK server!");
            }
            // Create node
            else if (EventType.NodeCreated == eventType) {
                logger.info("Node creation");
            }
            // Update node
            else if (EventType.NodeDataChanged == eventType) {
                logger.info("Node data update");
            }
            // Update child nodes
            else if (EventType.NodeChildrenChanged == eventType) {
                logger.info("Child node change");
            }
            // Delete node
            else if (EventType.NodeDeleted == eventType) {
                logger.info("node " + path + " Deleted");
            } else
                ;
        }

        else if (KeeperState.Disconnected == keeperState) {
            logger.info("And ZK Server Disconnected");
        } else if (KeeperState.AuthFailed == keeperState) {
            logger.info("Permission check failed");
        } else if (KeeperState.Expired == keeperState) {
            logger.info("Session failure");
        }
    }

    // CLOSE CLIENT CONNECTION
    public void close() throws InterruptedException {
        _zooKeeper.close();
    }

    // ****************************************************************
    // main
    // ***************************************************************
    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs-client.log", Level.INFO);
            demo = new Application();
            demo.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
