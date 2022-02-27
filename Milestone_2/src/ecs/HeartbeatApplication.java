package ecs;

import org.apache.log4j.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

import app_kvServer.KVServer;
import ecs.ZooKeeperApplication;

public class HeartbeatApplication implements Watcher {
    private static Logger logger = Logger.getRootLogger();

    KVServer kvServer;
    ZooKeeper zk;
    String serverName;

    public HeartbeatApplication(KVServer kvServer, ZooKeeper zk, String serverName) {
        this.kvServer = kvServer;
        this.zk = zk;
        this.serverName = serverName;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("HEARTBEAT WATCHER NOTIFICATION!");
        if (event == null) {
            return;
        }

        KeeperState keeperState = event.getState(); // Get connection status
        EventType eventType = event.getType(); // Event type
        String path = event.getPath(); // Affected path

        if (KeeperState.SyncConnected == keeperState) {
            // Delete node
            if (EventType.NodeDeleted == eventType) {
                logger.info("node " + path + " Deleted");
                kvServer.close();
            }
        } else if (KeeperState.Disconnected == keeperState || KeeperState.AuthFailed == keeperState
                || KeeperState.Expired == keeperState) {
            logger.info("Session failure");
            kvServer.close();
        }

        try {
            zk.exists(path, this);
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Unable to reset heartbeat watcher for server " + serverName);
            System.exit(1);
        }
    }
}