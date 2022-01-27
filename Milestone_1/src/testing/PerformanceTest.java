package testing;

import client.KVStore;

public class PerformanceTest {

    private KVStore kvStore;
    private static final int numRequests = 2000;

    public PerformanceTest() {
        try {
            kvStore = new KVStore("localhost", 50000);
        } catch (Exception e) {
            System.err.println(e.message());
        }
    }

    public void runTest() {
        // Iterate to test different ratios of PUT and GET requests to the server
        float[] ratioListPUT = [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2];
        for (int i = 0; i < ratioListPUT.length; i++) {
            for (int j = 0; j < numRequests; j++) {
                
            }
        }
    }

    public static void main(String[] args)  {
        new PerformanceTest().runTests();
    }

}