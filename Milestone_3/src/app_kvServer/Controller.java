package app_kvServer;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.net.Socket;
import java.io.OutputStream;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import logging.ServerLogSetup;

import ecs.ECSNode;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import shared.Utils;

public class Controller {
    private static Logger logger = Logger.getRootLogger();

    private final int NUM_REPLICANTS = 2;

    private HashMap<String, ECSNode> replicants = new HashMap<String, ECSNode>();
    private String controllerName;
    private String controllerHost;
    private int controllerPort;
    private KVServer kvServer;
    private HashMap<String, BigInteger> hashRing = new HashMap<String, BigInteger>();

    private Utils utils = new Utils();

    public Controller(KVServer kvServer) throws Exception {
        new ServerLogSetup("logs/Controller.log", Level.ALL);

        this.kvServer = kvServer;

        this.controllerName = kvServer.serverName;
        this.controllerPort = kvServer.getPort();
        this.controllerHost = kvServer.getHostname();
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

        this.hashRing = hashRing;

        logger.debug("My name is " + this.controllerName);
        logger.debug("hashRing length " + hashRing.size());
        for (Map.Entry<String, BigInteger> entry : hashRing.entrySet()) {
            logger.debug(entry.getKey() + " " + entry.getValue());
        }

        HashMap<String, ECSNode> prevReplicateServers = new HashMap<String, ECSNode>();
        // Store previous replicates
        if (replicants != null) {
            prevReplicateServers = new HashMap<String, ECSNode>(replicants);
            this.replicants.clear();
        } else {
            logger.debug("replicants is null");
        }

        ArrayList<BigInteger> orderedKeys = new ArrayList<>(hashRing.values());
        Collections.sort(orderedKeys);
        BigInteger currHash = hashRing.get(getNamePortHost());
        logger.debug("in setReplicationServers: " + getNamePortHost() + " w/ curr hash " + currHash);

        // If it's the only server in the hashring, no replicates
        if (orderedKeys.size() == 1) {
            logger.info("No replicates possible: only 1 node in hashring");
        } else {
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
            } else {
                Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
                namePortHost = getServerByHash(hashRing, orderedKeys.get(secondIdx));
                String[] replicant2Info = namePortHost.split(":");

                // put info into ECSNode
                ECSNode secondReplicant = new ECSNode(replicant2Info[0], replicant2Info[1], replicant2Info[2]);
                secondReplicant.setReplicateReceiverPort(replicateReceiverPorts.get(namePortHost));
                replicants.put(namePortHost, secondReplicant);
            }
        }

        // Array to store which replicates are new and need to be initialized
        ArrayList<ECSNode> needToInit = new ArrayList<>();
        // Determine which

        logger.debug("prevReplicateServers1 length " + prevReplicateServers.size());
        for (Map.Entry<String, ECSNode> entry : prevReplicateServers.entrySet()) {
            logger.debug(entry.getKey());
        }
        logger.debug("replicants1 length " + replicants.size());
        for (Map.Entry<String, ECSNode> entry : replicants.entrySet()) {
            logger.debug(entry.getKey());
        }

        for (Map.Entry<String, ECSNode> entry : replicants.entrySet()) {
            String rNamePortHost = entry.getKey();
            ECSNode r = entry.getValue();
            // If the old replicate is still a replicate,
            // remove it from prevReplicateServers
            if (prevReplicateServers.get(rNamePortHost) != null) {
                prevReplicateServers.remove(rNamePortHost);
            } else {
                needToInit.add(r);
            }
        }

        logger.debug("prevReplicateServers2 length " + prevReplicateServers.size());
        for (Map.Entry<String, ECSNode> entry : prevReplicateServers.entrySet()) {
            logger.debug(entry.getKey());
        }
        logger.debug("needToInit");
        for (ECSNode entry : needToInit) {
            logger.debug(entry.getNodeName());
        }

        // SEND A DELETE MSG TO THESE replicates
        this.deleteReplicates(new ArrayList<ECSNode>(prevReplicateServers.values()));

        // These replicates were just added, send them an init message
        this.initReplicates(needToInit);
    }

    public void updateReplicasOnMoveData() {
        for (ECSNode repl : this.replicants.values()) {
            // delete old stores
            CyclicBarrier barrier = new CyclicBarrier(1);
            logger.info("Deleting old info from replicate on MOVEDATA: " + repl.getNodePort());
            ControllerSender controllerDelete = new ControllerSender(repl, kvServer, barrier,
                    "", "delete");
            new Thread(controllerDelete).start();

            // wait before sending init
            try {
                TimeUnit.SECONDS.sleep(2);
                // init new stores with cut data
                CyclicBarrier barrier_2 = new CyclicBarrier(1);
                logger.info("Sending new info from replicate on MOVEDATA: " + repl.getNodePort());
                ControllerSender controllerInit = new ControllerSender(repl, kvServer, barrier_2,
                        this.controllerName + "@" + kvServer.getAllFromStorage(), "init");
                new Thread(controllerInit).start();
            } catch (Exception e) {
                logger.error("Unable to init replicates on MoveData");
            }

        }
    }

    public void initReplicates(ArrayList<ECSNode> replicates) {
        for (ECSNode replicate : replicates) {
            CyclicBarrier barrier = new CyclicBarrier(1);
            logger.info("INITIALIZING REPLICATE: " + replicate.getNodePort());
            ControllerSender controllerSender = new ControllerSender(replicate, kvServer, barrier,
                    this.controllerName + "@" + kvServer.getAllFromStorage(), "init");
            new Thread(controllerSender).start();
        }
    }

    public void updateReplicates() {
        for (ECSNode repl : this.replicants.values()) {
            CyclicBarrier barrier = new CyclicBarrier(1);
            logger.info("UPDATING REPLICATE: " + repl.getNodePort());
            ControllerSender controllerSender = new ControllerSender(repl, kvServer, barrier,
                    kvServer.getStringLogs(true), "update");
            new Thread(controllerSender).start();
        }
    }

    public void deleteReplicates(ArrayList<ECSNode> replicas) {
        // get list of replicas
        for (ECSNode repl : replicas) {
            CyclicBarrier barrier = new CyclicBarrier(1);
            logger.info("DELETING REPLICATE: " + repl.getNodePort());
            ControllerSender controllerSender = new ControllerSender(repl, kvServer, barrier,
                    "", "delete");
            new Thread(controllerSender).start();
        }
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
        // "127.0.0.1" is harded coded and required due to legacy code, however, ut
        // don't actually use the host for anything important
        return this.controllerName + ":" + this.controllerPort + ":127.0.0.1";
    }
}
