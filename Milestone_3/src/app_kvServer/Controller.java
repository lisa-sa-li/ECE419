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

public class Controller {
    private static Logger logger = Logger.getRootLogger();

    private final int NUM_REPLICANTS = 2;

    private HashMap<String, ECSNode> replicants;
    private HashMap<String, PersistantStorage> persistantStorages;
    private String controllerName;
    private String controllerHost;
    private int controllerPort;

    public Controller(String controllerName, int controllerPort, String controllerHost) {
        this.controllerName = controllerName;
        this.controllerPort = controllerPort;
        this.controllerHost = controllerHost;
    }

    public Controller(String controllerName, int controllerPort) {
        this.controllerName = controllerName;
        this.controllerPort = controllerPort;
        this.controllerHost = "127.0.0.1";
    }

    public boolean sameReplicateServers() {
        // figure out which servers are replicators (checks if same as before)
        // true if same as before, false if diff
        return true;
    }

    public int getNumReplicants() {
        return replicants.size();
    }

    public HashMap<String, ECSNode> getReplicateServers() {
        return replicants;
    }

    public void setReplicationServers(HashMap<String, BigInteger> hashRing,
            HashMap<String, Integer> replicateReceiverPorts) {
        // clear current replication servers
        // this.replicants.clear();
        // check that current server exists in the hashring!
        // edge cases: only 1 node in ring, only 2 nodes in ring

        Collection<BigInteger> keys = hashRing.values();
        ArrayList<BigInteger> orderedKeys = new ArrayList<>(keys);
        Collections.sort(orderedKeys);
        BigInteger currHash = hashRing.get(getNamePortHost());

        // If it's the only server in the hashring, no replicates
        if (orderedKeys.size() == 1) {
            logger.info("No replicates possible: only 1 node in hashring");
            return;
        }

        // find first replicant
        Integer firstIdx = orderedKeys.indexOf(currHash);
        firstIdx = (firstIdx + 1) % orderedKeys.size();
        String namePortHost = getServerByHash(hashRing, orderedKeys.get(firstIdx));
        String[] replicant1Info = namePortHost.split(":");

        // put info into ECSNode
        ECSNode firstReplicant = new ECSNode(replicant1Info[0], replicant1Info[1], replicant1Info[2]);
        firstReplicant.setReplicateReceiverPort(replicateReceiverPorts.get(namePortHost));
        replicants.put(namePortHost, firstReplicant);

        // check if second replicant possible
        if (orderedKeys.size() == 2) {
            logger.info("Only 1 replicate possible: 2 nodes total in the ring");
            return;
        }

        Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
        namePortHost = getServerByHash(hashRing, orderedKeys.get(secondIdx));
        String[] replicant2Info = namePortHost.split(":");

        // put info into ECSNode
        ECSNode secondReplicant = new ECSNode(replicant2Info[0], replicant2Info[1], replicant2Info[2]);
        secondReplicant.setReplicateReceiverPort(replicateReceiverPorts.get(namePortHost));
        replicants.put(namePortHost, secondReplicant);
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

    public String getNamePortHost() {
        return this.controllerName + ":" + this.controllerPort + ":" + this.controllerHost;
    }

}
