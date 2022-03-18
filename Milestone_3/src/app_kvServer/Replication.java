package app_kvServer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Objects;
import java.util.Map.Entry;

import ecs.ECSNode;

public class Replication {

    private final int NUM_REPLICANTS = 2;

    ArrayList<ECSNode> replicants;
    private String serverName;
    private String serverHost;
    private int serverPort;


    public Replication(String serverName, int serverPort, String serverHost){
        this.serverName = serverName;
        this.serverPort = serverPort;
        this.serverHost = serverHost;
    }

    public Replication(String serverName, int serverPort){
        this.serverName = serverName;
        this.serverPort = serverPort;
        this.serverHost = "127.0.0.1";
    }

    public boolean sameReplicateServers(){
        // figure out which servers are replicators (checks if same as before)
        // true if same as before, false if diff
        return true;
    }

    public void getReplicationServers(HashMap<String, BigInteger> hashRing){
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
        replicants.add(firstReplicant);

        // check if second replicant possible
        if (orderedKeys.size() == 2){
            logger.info("Only 1 replicant possible: 2 nodes total in the ring");
            return;
        }

		Integer secondIdx = (firstIdx + 1) % orderedKeys.size();
        String[] replicant2Info = getServerByHash(hashRing, orderedKeys.get(secondIdx)).split(":");

        // put info into ECSNode
        ECSNode secondReplicant = new ECSNode(replicant2Info[0], replicant2Info[1], replicant2Info[2]);
        replicants.add(secondReplicant);
    }

    // from https://stackoverflow.com/questions/1383797/java-hashmap-how-to-get-key-from-value
    public String getServerByHash(HashMap<String, BigInteger> map, BigInteger value) {
        for (Entry<String, BigInteger> entry : map.entrySet()) {
            if (Objects.equals(value, entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    public void clearReplicateData(){
        // clear replicated data
    }

    public String getNamePortHost() {
		String rval = this.serverName + ":" + this.serverPort + ":" + this.serverHost;
		return rval;
	}

    public void sendUpdatedReplicateData(){
        // send replicated data to replicate servers
    }

    public void getUpdatedReplicateData(){
        // get replicated data from head server
    }
    
}
