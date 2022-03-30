package ecs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map.Entry;
import java.security.KeyException;
import java.security.MessageDigest;

import java.util.*;

import org.apache.log4j.Logger;

import shared.exceptions.UnexpectedValueException;
import shared.messages.Metadata;
import shared.messages.Metadata.MessageType;
import shared.messages.JSONMessage;
import shared.Utils;

public class HashRing {
    private static final Logger logger = Logger.getLogger("hashring");
    private ArrayList<BigInteger> hashOrder = new ArrayList<>();
    private HashMap<String, BigInteger> hashRing = new HashMap<String, BigInteger>();
    private HashMap<String, Integer> ports = new HashMap<String, Integer>();
    private HashMap<String, String> serverInfo = new HashMap<String, String>();
    private HashMap<BigInteger, ECSNode> hashServers = new HashMap<BigInteger, ECSNode>();
    private HashMap<String, Integer> replicatePorts = new HashMap<String, Integer>();

    private int numServers = 0;
    private Utils utils = new Utils();

    public HashRing(HashMap<String, String> serverInfo) {
        this.serverInfo = serverInfo;
    }

    public HashRing() {
    }

    public void addNode(ECSNode newNode) {
        String name = newNode.getNodeName();

        // hash ip:port
        String toHash = newNode.getNodeHost() + ":" + Integer.toString(newNode.getNodePort());
        BigInteger hashed = getHash(toHash);

        // update lists + reorder
        hashOrder.add(hashed);
        Collections.sort(hashOrder);
        hashRing.put(name + ":" + newNode.getNodePort() + ":" + newNode.getNodeHost(), hashed);
        replicatePorts.put(name + ":" + newNode.getNodePort() + ":" + newNode.getNodeHost(),
                newNode.getReplicateReceiverPort());
        hashServers.put(hashed, newNode);

        numServers += 1;

        // collect idx of new node in hashring
        int idx = hashOrder.indexOf(hashed);

        // find previous node to get data from
        if (numServers == 0) {
            // There is no end hash because it's the only node in the ring
            newNode.setHashRange(hashed, null);
        } else {
            int prevIdx = idx == 0 ? numServers - 1 : idx - 1;
            ECSNode prevNode = hashServers.get(hashOrder.get(prevIdx));
            // give prev node end hash
            prevNode.setHashRange(prevNode.getHash(), hashed);

            newNode.setHashRange(hashed, prevNode.getHash());
            // send metadata to servers when there's more than 1 server operating

            if (numServers > 1) {
                Metadata update = new Metadata(MessageType.MOVE_DATA, hashRing, replicatePorts, newNode);
                prevNode.sendMessage(update);
                JSONMessage msg = prevNode.receiveMessage();
                // set hash
                updateAll(prevNode.getHash());
            } else {
                updateAll();
            }
        }
    }

    public void addNodes(ArrayList<ECSNode> nodes) throws Exception {
        for (ECSNode node : nodes) {
            try {
                addNode(node);
            } catch (Exception e) {
                throw new Exception("Could not add node: " + node.getNodeName());
            }
        }
    }

    public HashMap<String, ECSNode> getReplicationServers(String givenNamePortHost) {

        HashMap<String, ECSNode> replicates = new HashMap<String, ECSNode>();

        ArrayList<BigInteger> orderedKeys = new ArrayList<>(hashRing.values());
        Collections.sort(orderedKeys);
        // get hash from given namePortHost
        BigInteger currHash = hashRing.get(givenNamePortHost);

        // If it's the only server in the hashring, no replicates
        if (orderedKeys.size() == 1) {
            logger.info("No replicates possible: only 1 node in hashring");
            return null;
        } else {
            // get next index after currentHash
            Integer firstIdx = orderedKeys.indexOf(currHash);
            firstIdx = (firstIdx + 1) % orderedKeys.size();
            ECSNode replicate1 = hashServers.get(hashOrder.get(firstIdx));

            // put info into replicate list
            replicates.put(replicate1.getNamePortHost(), replicate1);

            // check if second replicant possible
            if (orderedKeys.size() == 2) {
                logger.info("Only 1 replicate possible: 2 nodes total in the ring");
            } else {
                Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
                ECSNode replicate2 = hashServers.get(hashOrder.get(secondIdx));

                replicates.put(replicate2.getNamePortHost(), replicate2);
            }
        }

        return replicates;
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

    public void removeNode(String name) {
        String portHost = serverInfo.get(name);
        // get hash from array list
        BigInteger removeHash = hashRing.get(name + ":" + portHost);
        if (removeHash == null) {
            throw new NullPointerException(
                    "Invalid node name: this node name is either incorrect or no longer in the hashring");
        }
        // collect idx of removed node in hashring
        int idx = hashOrder.indexOf(removeHash);
        // find current node
        ECSNode deadNode = hashServers.get(hashOrder.get(idx));
        // find previous node to send data to
        int prevIdx = idx == 0 ? numServers - 1 : idx - 1;
        ECSNode prevNode = hashServers.get(hashOrder.get(prevIdx));
        BigInteger finalEndHash = numServers == 2 ? null : deadNode.getEndHash();
        prevNode.setHashRange(prevNode.getHash(), finalEndHash);
        // remove values from lists
        hashOrder.remove(removeHash);
        hashRing.remove(name + ":" + portHost);
        replicatePorts.remove(name + ":" + portHost);
        hashServers.remove(removeHash);

        // send metadata to servers
        if (numServers > 1) {
            Metadata update = new Metadata(MessageType.SET_METADATA, hashRing, replicatePorts, null);
            prevNode.sendMessage(update);
        }
        Metadata death = new Metadata(MessageType.MOVE_DATA, hashRing, replicatePorts, prevNode);
        deadNode.sendMessage(death);

        numServers -= 1;

        // update all except dead node (which won't respond)
        updateAll(deadNode.getHash());
    }

    private void updateAll(BigInteger hashed) {
        // iterate through sorted key array
        for (BigInteger key : hashOrder) {
            if (key.compareTo(hashed) == 0) {
                continue;
            }
            ECSNode currNode = hashServers.get(key);
            Metadata metadata = new Metadata(MessageType.SET_METADATA, hashRing, replicatePorts, null);
            // send server info
            currNode.sendMessage(metadata);
            JSONMessage msg = currNode.receiveMessage();

        }
    }

    private void updateAll() {
        // iterate through sorted key array
        for (BigInteger key : hashOrder) {
            ECSNode currNode = hashServers.get(key);
            Metadata metadata = new Metadata(MessageType.SET_METADATA, hashRing, replicatePorts, null);
            // send server info
            currNode.sendMessage(metadata);
        }
    }

    public void startAll() {
        // iterate through sorted key array
        for (BigInteger key : hashOrder) {
            ECSNode currNode = hashServers.get(key);
            Metadata metadata = new Metadata(MessageType.START, hashRing, replicatePorts, null);
            // send server info
            currNode.sendMessage(metadata);
        }
    }

    public void stopAll() {
        // iterate through sorted key array
        for (BigInteger key : hashOrder) {
            ECSNode currNode = hashServers.get(key);
            Metadata metadata = new Metadata(MessageType.STOP, hashRing, replicatePorts, null);
            // send server info
            currNode.sendMessage(metadata);
        }
    }

    public HashMap<String, ECSNode> getHashRingMap() {
        // Convert the hash ring to {serverName: node}
        HashMap<String, ECSNode> hashRingtoServers = new HashMap<String, ECSNode>();

        Iterator<Map.Entry<String, BigInteger>> it = hashRing.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, BigInteger> pair = (Map.Entry) it.next();

            String name = pair.getKey().toString().split(":")[0];
            BigInteger hash = pair.getValue();
            ECSNode node = hashServers.get(hash);
            hashRingtoServers.put(name, node);
        }

        return hashRingtoServers;
    }

    public void recoverFromReplicas(String namePortHost) {
        // namePortHost must be passed into key or value
        // ECS determines who the replicas need to send
        // receiverNodePort needs to be REPLICANT receiver port, not client port

        if (numServers == 1) {
            // Sad, data is lost forever
            return;
        }

        HashMap<String, ECSNode> hashRingMap = getHashRingMap();
        ECSNode crashedNode = hashRingMap.get(namePortHost);
        BigInteger crashedHash = crashedNode.getHash();
        BigInteger crashedEndHash = crashedNode.getEndHash();

        // Update end hash of previous node
        int idx = hashOrder.indexOf(crashedHash);
        int prevIdx = idx == 0 ? numServers - 1 : idx - 1;
        ECSNode prevNode = hashServers.get(hashOrder.get(prevIdx));
        prevNode.setHashRange(prevNode.getHash(), crashedEndHash);
        // This is how we communicate the crash node's name to the replica
        prevNode.setNodeName(prevNode.getNodeName() + "@" + namePortHost);

        HashMap<String, ECSNode> replicates = getReplicationServers(namePortHost);
        // The prevNode tells the replica who to send their replica data to
        Metadata recover = new Metadata(MessageType.RECOVER, hashRing, prevNode, true);

        for (Map.Entry<String, ECSNode> entry : replicates.entrySet()) {
            String rNamePortHost = entry.getKey();
            ECSNode r = entry.getValue();

            r.sendMessage(recover);
            // JSONMessage msg = prevNode.receiveMessage();
        }
    }

    public boolean isEmpty() {
        return hashRing.size() == 0;
    }

    public BigInteger getHash(String value) {
        return utils.getHash(value);
    }

}
