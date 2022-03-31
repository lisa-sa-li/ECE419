package app_kvServer;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
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

    private PersistantStorage ps;
    private String replicateName;
    private String replicateHost;
    private int replicatePort;
    private String masterName;
    private int masterPort;
    private String masterHost;

    public Replicate(String replicateName, int replicatePort, String replicateHost) {
        this.replicateName = replicateName;
        this.replicatePort = replicatePort;
        this.replicateHost = replicateHost;
    }

    public void setMasterName(String masterName) {
        this.masterName = masterName;
    }

    public void setMasterPort(int masterPort) {
        this.masterPort = masterPort;
    }

    public void setMasterHost(String masterHost) {
        this.masterHost = masterHost;
    }

    public String getMasterNamePortHost() {
        return this.masterName + ":" + this.masterPort + ":" + this.masterHost;
    }

    public void initReplicateData(String data) {
        // Create its persistant storage
        String[] splitData = data.split("@", 2);
        masterName = splitData[0];
        ps = new PersistantStorage("repl_" + this.masterName + "_" + getNamePortHost());
        ps.clearStorage();
        ps.appendToStorage(splitData[1]);
    }

    public void updateReplicateData(String data) {
        String[] splitData = data.split("@", 2);
        masterName = splitData[0];

        if (splitData[1].isEmpty()) {
            return;
        }
        if (ps == null) {
            ps = new PersistantStorage("repl_" + masterName + "_" + getNamePortHost());
        }
        String[] ops = splitData[1].split("\n");
        for (String line : ops) {
            JSONMessage msg = new JSONMessage();
            msg.deserialize(line);

            String key = msg.getKey();
            String value = msg.getValue();
            StatusType status = msg.getStatus();

            try {
                ps.put(key, value);
            } catch (Exception e) {
                logger.info("PUT_ERROR in replicate: key " + key + " & value " + value);
            }
        }
    }

    public void clearReplicateData() {
        ps.clearStorage();
    }

    public String getAllReplicateData() {
        return ps.getAllFromStorage();
    }

    public String getKVFromReplica(String key) throws Exception {
        return ps.get(key);
    }

    public void clearAllReplicateData() {
        ps.clearStorage();
    }

    public void deleteReplicateData(String data) {
        this.masterName = data;
        if (ps == null) {
            ps = new PersistantStorage("repl_" + masterName + "_" + getNamePortHost());
        }
        ps.deleteStorage();

    }

    public String getNamePortHost() {
        String rval = this.replicateName + ":" + this.replicatePort + ":" + this.replicateHost;
        return rval;
    }

}
