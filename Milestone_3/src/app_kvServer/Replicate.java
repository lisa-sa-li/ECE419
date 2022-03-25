package app_kvServer;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;
import java.net.Socket;
import java.io.OutputStream;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import logging.ServerLogSetup;

import ecs.ECSNode;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.PersistantStorage;

public class Replicate {
    private static Logger logger = Logger.getRootLogger();

    private HashMap<String, PersistantStorage> persistantStorages;
    private String replicateName;
    private String replicateHost;
    private int replicatePort;
    private String masterNamePortHost;

    public Replicate(String replicateName, int replicatePort, String replicateHost) {
        this.replicateName = replicateName;
        this.replicatePort = replicatePort;
        this.replicateHost = replicateHost;
    }

    public void setMaster(String masterNamePortHost) {
        this.masterNamePortHost = masterNamePortHost;
    }

    public void initReplicateData(String data) {
        // Create its persistant storage
        logger.debug("initReplicateData START");
        if (persistantStorages.size() == 2) {
            logger.error("This server already has 2 replicates");
            return;
        }
        logger.debug("NAME FOR REPLICA FILE PERSISTENT: " + "repl_" + masterNamePortHost + "_" + getNamePortHost());
        PersistantStorage ps = new PersistantStorage("repl_" + masterNamePortHost + "_" + getNamePortHost());
        logger.debug("CREATED STORAGE FOR REPLICA: " + data);
        ps.appendToStorage(data);
        persistantStorages.put(masterNamePortHost, ps);
    }

    public void updateReplicateData(String data) {
        PersistantStorage ps = persistantStorages.get(masterNamePortHost);

        for (String line : data.split("\n")) {
            JSONMessage msg = new JSONMessage();
            msg.deserialize(line);

            String key = msg.getKey();
            String value = msg.getValue();
            StatusType status = msg.getStatus();

            switch (status) {
                case PUT:
                    try {
                        ps.put(key, value);
                    } catch (Exception e) {
                        logger.info("PUT_ERROR in replicate: key " + key + " & value " + value);
                    }
                    break;
                default:
                    logger.error("Unknown command.");
                    break;
            }
        }
    }

    public void clearReplicateData() {
        if (persistantStorages.get(masterNamePortHost) == null) {
            logger.error("No replica with the name-port-host " + masterNamePortHost);
        } else {
            PersistantStorage ps = persistantStorages.get(masterNamePortHost);
            ps.clearStorage();
        }
        return;
    }

    public void clearAllReplicateData() {
        for (PersistantStorage ps : persistantStorages.values()) {
            ps.clearStorage();
        }
    }

    public void deleteReplicateData() {
        if (persistantStorages.get(masterNamePortHost) == null) {
            logger.error("No replica with the name-port-host " + masterNamePortHost);
        } else {
            PersistantStorage ps = persistantStorages.get(masterNamePortHost);
            ps.deleteStorage();
        }
        return;
    }

    public void deleteAllReplicateData() {
        for (PersistantStorage ps : persistantStorages.values()) {
            ps.clearStorage();
        }
    }

    public String getNamePortHost() {
        String rval = this.replicateName + ":" + this.replicatePort + ":" + this.replicateHost;
        return rval;
    }

}
