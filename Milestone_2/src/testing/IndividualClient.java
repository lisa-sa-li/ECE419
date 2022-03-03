package testing;

import java.lang.Math;
import java.util.Random;
import client.KVStore;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;


public class IndividualClient implements Runnable {
    int numPUTRequests;
    int numGETRequests;
    private KVStore kvStore;
    private List<ArrayList<String>> associatedData;
    Random randomNumber = new Random();
    private boolean initialPopulating;
    private List<ArrayList<String>> originalData;
    private int numRequests;
    private double totalBytes = 0;

    public IndividualClient(String hostname, int port, List<ArrayList<String>> associatedData,
                            List<ArrayList<String>> originalData, int numRequests, boolean initialPopulating) {
        this.kvStore = new KVStore(hostname, port);
        this.associatedData = associatedData;
        this.initialPopulating = initialPopulating;
        this.originalData = originalData;
        this.numRequests = numRequests;
        System.out.println("associatedData.length " + associatedData.size());
    }

    public double getTotalBytes(){
        return this.totalBytes;
    }

    public void run() {
        try{
            System.out.println("start of individual client run() ");
            try {
                this.kvStore.connect();
                System.out.println("KVStore connected ");
                // Had to comment out initHeartBeat() in KVServer.java
            } catch (Exception e) {
                System.out.println("KVStore not connected: " + e);
            }
            int count = 0;
            for (ArrayList<String> keyValuePair : this.associatedData) {
                String key = keyValuePair.get(0);
                String value = keyValuePair.get(1);
                if (this.initialPopulating) {
                    try {
                        this.kvStore.put(key, value);
                        count += 1;
                    } catch (Exception e) {
                    }
                    if (count % 100 == 0){
                        System.out.println("Key value pair count " + count);
                    }
                } else {
                    if (Math.random() <= 0.5) {
                        try {
                            this.kvStore.put(key, value);
                            this.totalBytes += value.getBytes(StandardCharsets.UTF_8).length;
                        } catch (Exception e) {
                        }
                    } else {
                        int randomIndex = this.randomNumber.nextInt(this.originalData.size());
                        ArrayList<String> pair = this.originalData.get(randomIndex);
                        try {
                            String returnVal = this.kvStore.get(pair.get(0)).getValue();
                            this.totalBytes += returnVal.getBytes(StandardCharsets.UTF_8).length;
                        } catch (Exception e){
                        }
                    }
                }
                if (this.numPUTRequests + this.numGETRequests == this.numRequests) {
                    break;
                }
            }
            this.kvStore.disconnect();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
