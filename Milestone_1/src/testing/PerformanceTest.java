package testing;

import app_kvServer.KVServer;
import shared.messages.KVMessage.StatusType;

public class PerformanceTest {

    private KVServer kvServer;
    private static final int numRequests = 2000;
    private long totalDurationPUT = 0;
    private long totalDurationGET = 0;
    private int numPUTInsertRequests = 0;
    private int numPUTUpdateRequests = 0;
    private int numGETRequests = 0;


    public PerformanceTest() {
        try {
            this.kvServer = new KVServer(8080, 0, "");
        } catch (Exception e) {
            System.out.println("Couldn't connect to server. " + e);
        }
    }

    public void computeDurationPUT(String key, String val) {
        long startTime = System.currentTimeMillis();
        try {
            StatusType outputStatus = this.kvServer.putKV(key, val);
        } catch (Exception e) {
            System.out.println("Error when doing PUT request. " + e);
        }
        long endTime = System.currentTimeMillis();
        totalDurationPUT += (endTime - startTime) / 1000;
        if (val != "null") {
            numPUTInsertRequests++;
        } else {
            numPUTUpdateRequests++;
        }
    }

    public void computeDurationGET(String key) {
        long startTime = System.currentTimeMillis();
        try {
            String getOutput = this.kvServer.getKV(key);
        } catch (Exception e) {
            System.out.println("Error when doing GET request. " + e);
        }
        long endTime = System.currentTimeMillis();
        totalDurationGET += (endTime - startTime) / 1000;
        numGETRequests++;
    }

    public void runTests() {
        // Iterate to test different ratios of PUT and GET requests to the server
        double[] ratioListPUT = {0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2};
        for (int i = 0; i < ratioListPUT.length; i++) {
            for (int j = 0; j < numRequests; j++) {
                System.out.println("IN loop");
                
            }
        }
    }


    public static void main(String[] args) {
        new PerformanceTest().runTests();
    }

}

