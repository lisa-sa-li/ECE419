package app_kvECS;

import org.apache.log4j.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.KeeperException;

import app_kvServer.KVServer;
import ecs.ZooKeeperApplication;
import ecs.IECSNode.NodeStatus;

public class HeartbeatWatcher implements Watcher {
    private static Logger logger = Logger.getRootLogger();

    ECSClient ecsClient;
    String serverName;

    public HeartbeatWatcher(ECSClient ecsClient) {
        this.ecsClient = ecsClient;
    }

    @Override
    public void process(WatchedEvent event) {
        if (EventType.NodeDeleted == event.getType()) {
            logger.debug("event.getPath() " + event.getPath());
            String name = event.getPath().split("/")[1];
            String namePortHost = name + ":" + ecsClient.serverInfo.get(name);

            NodeStatus status = ecsClient.currServerMap.get(name).getStatus();

            if (status != NodeStatus.SHUTTING_DOWN && status != NodeStatus.OFFLINE) {
                logger.info("HEARTBEAT DIED: Server " + name + " is dead");

                // SEND MESSAGE TO REPLICAS TO RECOVER
                ecsClient.hashRing.recoverFromReplicas(namePortHost);

                ecsClient.allServerMap.remove(name);
                ecsClient.currServerMap.remove(name);
                // Update all servers' metadata
                ecsClient.hashRing.removeNode(name);

                // Replaced with a new storage server with default cache info
                ecsClient.addNode("FIFO", 3);
            }
        }
    }
}