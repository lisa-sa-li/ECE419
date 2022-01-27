package testing;

import client.KVStore;


public class PerformanceTest {

    private KVServer server;
    private static final int numRequests = 2000;

    public PerformanceTest() {

    }

    public void testStub() {
        assertTrue(true);
    }

    public static void main(String[] args)  {
        new PerformanceTest().runTest();
    }

}