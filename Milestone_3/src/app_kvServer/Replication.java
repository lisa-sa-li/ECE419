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

public class Replication {
    private static Logger logger = Logger.getRootLogger();

    private final int NUM_REPLICANTS = 2;

    private HashMap<String, ECSNode> replicants;
    private HashMap<String, PersistantStorage> persistantStorages;
    private String serverName;
    private String serverHost;
    private int serverPort;

    public Replication(String serverName, int serverPort, String serverHost) {
        this.serverName = serverName;
        this.serverPort = serverPort;
        this.serverHost = serverHost;
    }

    public Replication(String serverName, int serverPort) {
        this.serverName = serverName;
        this.serverPort = serverPort;
        this.serverHost = "127.0.0.1";
    }

    public boolean sameReplicateServers() {
        // figure out which servers are replicators (checks if same as before)
        // true if same as before, false if diff
        return true;
    }

    public void getReplicationServers(HashMap<String, BigInteger> hashRing) {
        // clear current replication servers
        this.replicants.clear();
        // check that current server exists in the hashring!
        // edge cases: only 1 node in ring, only 2 nodes in ring

        Collection<BigInteger> keys = hashRing.values();
        ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
        Collections.sort(orderedKeys);
        BigInteger currHash = hashRing.get(getNamePortHost());

        // If it's the only server in the hashring, no replicates
        if (orderedKeys.size() == 1) {
            logger.info("No replicants possible: only 1 node in hashring");
            return;
        }

        // find first replicant
        Integer firstIdx = orderedKeys.indexOf(currHash);
        firstIdx = (firstIdx + 1) % orderedKeys.size();
        String[] replicant1Info = getServerByHash(hashRing, orderedKeys.get(firstIdx)).split(":");

        // put info into ECSNode
        ECSNode firstReplicant = new ECSNode(replicant1Info[0], replicant1Info[1], replicant1Info[2]);
        String namePortHost = "" + replicant1Info[0] + ":" + replicant1Info[1] + ":" + replicant1Info[2];
        replicants.put(namePortHost, firstReplicant);

        // check if second replicant possible
        if (orderedKeys.size() == 2) {
            logger.info("Only 1 replicant possible: 2 nodes total in the ring");
            return;
        }

        Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
        String[] replicant2Info = getServerByHash(hashRing, orderedKeys.get(secondIdx)).split(":");

        // put info into ECSNode
        ECSNode secondReplicant = new ECSNode(replicant2Info[0], replicant2Info[1], replicant2Info[2]);
        namePortHost = "" + replicant2Info[0] + ":" + replicant2Info[1] + ":" + replicant2Info[2];
        replicants.put(namePortHost, secondReplicant);
    }

    public void initReplicateData(String namePortHost) {
        // Create its persistant storage
        if (persistantStorages.size() == 2) {
            logger.error("This server already has 2 replicates");
            return;
        }
        persistantStorages.put(namePortHost, new PersistantStorage("repl_" + namePortHost + "_" + getNamePortHost()));
    }

    // from
    // https://stackoverflow.com/questions/1383797/java-hashmap-how-to-get-key-from-value
    public String getServerByHash(HashMap<String, BigInteger> map, BigInteger value) {
        for (Entry<String, BigInteger> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    public void clearReplicateData(String namePortHost) {
        if (persistantStorages.get(namePortHost) == null) {
            logger.error("No replica with the name-port-host " + namePortHost);
        } else {
            PersistantStorage ps = persistantStorages.get(namePortHost);
            ps.clearStorage();
        }
        return;
    }

    public void clearAllReplicateData() {
        for (PersistantStorage ps : persistantStorages.values()) {
            ps.clearStorage();
        }
    }

    public void deleteReplicateData(String namePortHost) {
        if (persistantStorages.get(namePortHost) == null) {
            logger.error("No replica with the name-port-host " + namePortHost);
        } else {
            PersistantStorage ps = persistantStorages.get(namePortHost);
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
        String rval = this.serverName + ":" + this.serverPort + ":" + this.serverHost;
        return rval;
    }

    public void sendUpdatedReplicateData() {
        // send replicated data to replicate servers (replicate servers are the same)

        // TODO: check that replicate servers haven't changed
        // if !serversChanged)

        // do we need this?
        // lockWrite();

        // for (ECSNode replicate : replicants.values()) {
        // String hostOfReceiver = replicate.getNodeHost();
        // String nameOfReceiver = replicate.getNodeName();

        // try {
        // Socket socket = new Socket(hostOfReceiver, portOfReceiver);
        // OutputStream output = socket.getOutputStream();

        // JSONMessage json = new JSONMessage();
        // json.setMessage(StatusType.PUT_MANY.name(), "put_many", dataInRange, null);
        // byte[] jsonBytes = json.getJSONByte();

        // output.write(jsonBytes, 0, jsonBytes.length);
        // output.flush();
        // output.close();
        // socket.close();
        // } catch (Exception e) {
        // logger.error("Unable to send data to replicant " + nameOfReceiver + ", " +
        // e);
        // }
        // }
        // }

        // public void getUpdatedReplicateData() {
        // // get replicated data from head server
        // }
    }

}
