package ecs;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.security.KeyException;
import java.security.MessageDigest;

import org.apache.log4j.Logger;

import shared.exceptions.UnexpectedValueException;

public class HashRing {
    private static final Logger logger = Logger.getLogger("hashring");
    private HashMap<BigInteger, ECSNode> hashServers = new HashMap<BigInteger, ECSNode>();
    private ArrayList<BigInteger> hashOrder = new ArrayList<>();
    private HashMap<String, BigInteger> hashRing = new HashMap<String, BigInteger>();
    private int numServers = 0;


    public void createHashRing(HashMap<String,ECSNode> currServers){
        // this function should only be called once per execution
        if (hashOrder.size() != 0 || hashRing.size() != 0){
            throw new UnexpectedValueException("This function cannot be called twice");
            return;
        }
        int numCurrServers = currServers.size();

        if (numCurrServers == 0){
            // no active nodes
            logger.error("No current servers to construct hashring");
            return;
        }

        // construct the ring from name hash values
        for(ECSNode node: currServers.values()){
            String name = node.getNodeName();

            // hash ip:port
            String toHash = node.getNodeHost() + ":" + Integer.toString(node.getNodePort()); 
            BigInteger hashed = getHash(toHash);

            // let the server know its hash
            // node.setHash(hashed);

            // append to hashOrder + hashRing + hashServers
            hashOrder.add(hashed);
            hashRing.put(name, hashed);
            hashServers.put(hashed, node);
        }
        // sort the hashes
        Collections.sort(hashOrder);

        // set ranges
        setRanges();

        // set size
        numServers = hashOrder.size();
    }

    private void addNode(ECSNode node){
        String name = node.getNodeName();

        // hash ip:port
        String toHash = node.getNodeHost() + ":" + Integer.toString(node.getNodePort()); 
        BigInteger hashed = getHash(toHash);

        // update lists + reorder
        hashOrder.add(hashed);
        Collections.sort(hashOrder);
        hashRing.put(name, hashed);

        numServers += 1;
    }

    private void addNodes(ArrayList<ECSNode> nodes){
        for (ECSNode node: nodes){
            try{
                addNode(node);
            } catch (Exception e){
                throw new Exception("Could not add node: "+ node.getNodeName());
                return;
            }
        }
    }

    private void removeNode(String name){
        // get hash from array list
        BigInteger removeHash = hashRing.get(name);
        if (removeHash == null){
            throw new NullPointerException("Invalid node name");
        }

        // remove values
        hashOrder.remove(removeHash);
        hashRing.remove(name);

        numServers -= 1;
    }

    private void setRanges(){
        // iterate through sorted key array
        int idx = 0;
        for (BigInteger key: hashOrder){
            ECSNode currNode = hashServers.get(key);
            int endIdx = (idx+1)%numServers;

            currNode.setHashRange(key, hashOrder.get(endIdx));
        }
    }

    private BigInteger getHash(String value){
        try {
            // get message bytes
            byte[] byteVal = value.getBytes("UTF-8");
            // create md5 instance
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            // convert value to md5 hash (returns bytes)
            byte[] mdDigest = md5.digest(byteVal);

            // convert to string
            StringBuilder stringHash = new StringBuilder();
            for (byte b : mdDigest) {
                // code below: modified code from https://stackoverflow.com/questions/11380062/what-does-value-0xff-do-in-java
                stringHash.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
                System.out.println("HERE IS THE HASH VAL: " + stringHash);
            }
            // return stringHash.toString();
            // return hex biginteger
            return new BigInteger(stringHash.toString(), 16);

        } catch (Exception e) {
            return new BigInteger("00000000000000000000000000000000");
        }
    }

}
