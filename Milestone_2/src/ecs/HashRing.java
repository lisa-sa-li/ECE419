package ecs;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.security.MessageDigest;

import org.apache.log4j.Logger;

public class HashRing {
    private static final Logger logger = Logger.getLogger("hashring");
    private ArrayList<ECSNode> hashServers = new ArrayList<>();
    private ArrayList<BigInteger> hashOrder = new ArrayList<>();
    private HashMap<String, BigInteger> hashRing = new HashMap<String, BigInteger>();


    public void createHashRing(HashMap<String,ECSNode> currServers){
        // this function should only be called once per execution
        int numServers = currServers.size();

        if (numServers == 0){
            // no active nodes
            logger.error("No current servers to construct hashring");
            return;
        }

        // construct the ring from name hash values
        for(ECSNode node: currServers.values()){
            String name = node.getNodeName();
            nameHash = 

        }
    }

    private String getHash(String value){
        try {
            // get message bytes
            byte[] byteVal = value.getBytes("UTF-8");
            // create md5 instance
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            // convert value to md5 hash (returns bytes)
            byte[] mdDigest = md5.digest(byteVal);

            // convert to string
            StringBuilder sb = new StringBuilder();
            for (byte b : mdDigest) {
                sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            return sb.toString();

        } catch (Exception e) {
            return "00000000000000000000000000000000";
        }
    }

}
