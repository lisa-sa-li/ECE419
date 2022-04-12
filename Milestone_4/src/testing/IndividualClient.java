package testing;

import java.lang.Math;
import java.util.Random;
import client.KVStore;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class IndividualClient implements Runnable {
    int numPUTRequests = 0;
    int numGETRequests = 0;
    int numRecoverRequests = 0;
    private KVStore kvStore;
    private List<ArrayList<String>> associatedData;
    Random randomNumber = new Random();
    private boolean initialPopulating;
    private List<ArrayList<String>> otherData;
    private int numRequests;
    private double totalBytes = 0;
    private int waitCount = 10;

    public IndividualClient(String hostname, int port, List<ArrayList<String>> associatedData,
            List<ArrayList<String>> originalData, int numRequests, boolean initialPopulating) {
        this.kvStore = new KVStore(hostname, port);
        this.associatedData = associatedData;
        this.initialPopulating = initialPopulating;
        this.otherData = originalData;
        this.numRequests = numRequests;
        System.out.println("associatedData.length " + associatedData.size());
    }

    public double getTotalBytes() {
        return this.totalBytes;
    }

    public void run() {
        try {
            // System.out.println("start of individual client run() ");
            try {
                this.kvStore.connect();
                // System.out.println("KVStore connected ");
                // Had to comment out initHeartBeat() in KVServer.java
            } catch (Exception e) {
                System.out.println("KVStore not connected: " + e);
            }
            int count = 0;
            // String status = "";
            for (ArrayList<String> keyValuePair : this.associatedData) {
                String key = keyValuePair.get(0);
                String value = keyValuePair.get(1);
                if (this.initialPopulating) {
                    try {
                        //System.out.println("before PUT");
                        //System.out.println("key: " + key);
                        //System.out.println("value length: " + value.length());
                        try {
                            TimeUnit.SECONDS.sleep(waitCount);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        // status = this.kvStore.put(key, value.substring(0, 10)).getStatus().toString();
                        this.kvStore.put(key, value.substring(0, 10));
                        //System.out.println("END OF PUT Status: " + status);
                        this.numPUTRequests += 1;
                        count += 1;
                    } catch (Exception e) {
                    }
                    if (count % 5 == 0 && count != 0){
                        System.out.println("Key value pair count " + count);
                    }
                    // System.out.println("-----------------------------------------------------------------");
                } else {
                    if (Math.random() <= 0.33) {
                        try {
                            try {
                                TimeUnit.SECONDS.sleep(waitCount);
                            } catch (Exception e) {
                                System.out.println(e);
                            }
                            // System.out.println("Calling PUT");
                            this.totalBytes += value.substring(0, 10).getBytes(StandardCharsets.UTF_8).length;
                            this.kvStore.put(key, value.substring(0, 10));
                            this.numPUTRequests += 1;
                            //count += 1;
                            //System.out.println("PUT is called: " + count);
                        } catch (Exception e) {
                        }
                    } else if (Math.random() <= 0.66) {
                        // System.out.println("Calling GET");
                        int randomIndex = this.randomNumber.nextInt(this.otherData.size());
                        ArrayList<String> pair = this.otherData.get(randomIndex);
                        try {
                            TimeUnit.SECONDS.sleep(waitCount);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        try {
                            String returnVal = this.kvStore.get(pair.get(0)).getValue();
                            this.totalBytes += returnVal.getBytes(StandardCharsets.UTF_8).length;
                            this.numGETRequests += 1;
                        } catch (Exception e) {
                        }
                    } else {
                        int randomIndex = this.randomNumber.nextInt(this.otherData.size());
                        ArrayList<String> pair = this.otherData.get(randomIndex);
                        try {
                            TimeUnit.SECONDS.sleep(waitCount);
                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        try {
                            this.totalBytes += value.substring(0, 10).getBytes(StandardCharsets.UTF_8).length;
                            String returnVal = this.kvStore.recover(pair.get(0)).getValue();
                            this.numRecoverRequests += 1;
                        } catch (Exception e) {
                        }
                    }
                    /*if (count % 5 == 0 && count != 0){
                        System.out.println("Key value pair count " + count);
                    }*/
                }
                if (this.numPUTRequests + this.numGETRequests + this.numRecoverRequests == this.associatedData.size()) {
                    break;
                }
            }
            // System.out.println("Done with this client");
            this.kvStore.disconnect();
            // System.out.println("Disconnected kvstore");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
