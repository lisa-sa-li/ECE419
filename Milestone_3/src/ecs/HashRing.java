package ecs;

import java.io.IOException;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import com.google.gson.Gson;
import java.util.HashMap;
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
        replicatePorts.put(name + ":" + newNode.getNodePort() + ":" + newNode.getNodeHost(), newNode.getReplicateReceiverPort());
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

    public boolean isEmpty() {
        return hashRing.size() == 0;
    }

    public BigInteger getHash(String value) {
        return utils.getHash(value);
    }

}
