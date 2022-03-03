package testing;

import java.lang.Math;
import java.util.Random;
import client.KVStore;
import java.util.ArrayList;
import java.util.List;

public class IndividualClient implements Runnable {
    double totalDurationPUT;
    double totalDurationGET;
    int numPUTRequests;
    int numGETRequests;
    private KVStore kvStore;
    private List<ArrayList<String>> associatedData;
    Random randomNumber = new Random();
    private boolean initialPopulating;
    private List<ArrayList<String>> originalData;
    private int numRequests;

    public IndividualClient(String hostname, int port, List<ArrayList<String>> associatedData,
                            List<ArrayList<String>> originalData, int numRequests) {
        this.kvStore = new KVStore(hostname, port);
        this.associatedData = associatedData;
        this.initialPopulating = initialPopulating;
        this.originalData = originalData;
        this.numRequests = numRequests;
    }

    /*
    public double computeTimeDurationPUT(String key, String val) {
        long startTime = System.currentTimeMillis();
        try {
            this.kvStore.put(key, val);
        } catch (Exception e) {
        }
        long endTime = System.currentTimeMillis();
        return (endTime - startTime);
    }

    public double computeTimeDurationGET(String key) {
        long startTime = System.currentTimeMillis();
        try {
            this.kvStore.get(key);
        } catch (Exception e) {
        }
        long endTime = System.currentTimeMillis();
        return (endTime - startTime);
    }
    */
    public void run() {
        try{
            this.kvStore.connect();
            for (ArrayList<String> keyValuePair : this.associatedData) {
                String key = keyValuePair.get(0);
                String value = keyValuePair.get(1);
                if (this.initialPopulating) {
                    this.kvStore.put(key, value);
                } else {
                    if (Math.random() <= 0.5) {
                        try {
                            this.kvStore.put(key, value);
                        } catch (Exception e) {
                        }
                        // double outputDurationPUT = this.computeTimeDurationPUT(key, value);
                        // this.totalDurationPUT += outputDurationPUT;
                        // this.numPUTRequests += 1;
                    } else {
                        int randomIndex = this.randomNumber.nextInt(this.originalData.size());
                        ArrayList<String> pair = this.originalData.get(randomIndex);
                        try {
                            this.kvStore.get(pair.get(0));
                        } catch (Exception e){
                        }
                        // double outputDurationGET = this.computeTimeDurationGET(pair.get(0));
                        // this.totalDurationGET += outputDurationGET;
                        // this.numGETRequests += 1;
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
