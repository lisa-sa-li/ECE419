// package testing;

<<<<<<< HEAD
import app_kvServer.KVServer;
=======
// import client.KVStore;
>>>>>>> f611f6b3aaa84ccc7f35da03ee196a239f99d3ae

// public class PerformanceTest {

//     private KVServer kvServer;
//     private static final int numRequests = 2000;

<<<<<<< HEAD
    public PerformanceTest() {
        try {
            this.kvServer = new KVServer(8080, 0, "");
        } catch (Exception e) {
            System.out.println("Couldn't connect to server. " + e);
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
=======
//     public PerformanceTest() {
//         try {
//             this.kvServer = new KVServer("localhost", 50000);
//             this.kvServer.connect();
//         } catch (Exception e) {
//             System.out.println("Couldn't connect to server. ", e);
//         }
//     }

//     public void runTests() {
//         // Iterate to test different ratios of PUT and GET requests to the server
//         double[] ratioListPUT = {0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2};
//         for (int i = 0; i < ratioListPUT.length; i++) {
//             for (int j = 0; j < numRequests; j++) {
//                 System.out.println("IN loop");

//             }
//         }
//     }
>>>>>>> f611f6b3aaa84ccc7f35da03ee196a239f99d3ae

//     public static void main(String[] args) {
//         new PerformanceTest().runTests();
//     }

// }

