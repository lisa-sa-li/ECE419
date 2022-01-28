package testing;

import app_kvServer.KVServer;
import shared.messages.KVMessage.StatusType;

public class PerformanceTest {

    private KVServer kvServer;
    private static final int numRequests = 2000;
    private long totalDurationPUT;
    private int numPutInsertRequests;
    private int numPutUpdateRequests;


    public PerformanceTest() {
        try {
            this.kvServer = new KVServer(8080, 0, "");
        } catch (Exception e) {
            System.out.println("Couldn't connect to server. " + e);
        }
    }

    public void computeDurationPUT(String key, String val) {
        long startTime = System.nanoTime();
        try {
            StatusType outputStatus = this.kvServer.putKV(key, val);
        } catch (Exception e) {
            System.out.println("Error when doing PUT request. " + e);
        }
        long endTime = System.nanoTime();
        totalDurationPUT += (endTime - startTime) / 1000;
        if (val != "null") {
            numPutInsertRequests++;
        } else {
            numPutUpdateRequests++;
        }
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

