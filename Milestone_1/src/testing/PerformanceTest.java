package testing;

import app_kvServer.KVServer;
import shared.messages.KVMessage.StatusType;
import java.util.Random;
import java.lang.Math;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PerformanceTest {

    private KVServer kvServer;
    private static final int numRequests = 2000;
    double totalDurationPUT;
    double totalDurationGET;
    int numPUTRequests;
    int numGETRequests;
    Random randomNumber = new Random();

    public PerformanceTest() {
        try {
            this.kvServer = new KVServer(8082, 0, "");
        } catch (Exception e) {
            System.out.println("Couldn't connect to server. " + e);
        }
    }

    public double computeTimeDurationPUT(String key, String val) {
        long startTime = System.currentTimeMillis();
        try {
            StatusType outputStatus = this.kvServer.putKV(key, val);
        } catch (Exception e) {
            System.out.println("Error when doing PUT request. " + e);
        }
        long endTime = System.currentTimeMillis();
        numPUTRequests++;
        return (endTime - startTime) / 1000;
    }

    public double computeTimeDurationGET(String key) {
        long startTime = System.currentTimeMillis();
        try {
            String getOutput = this.kvServer.getKV(key);
        } catch (Exception e) {
            System.out.println("Error when doing GET request. " + e);
        }
        long endTime = System.currentTimeMillis();
        numGETRequests++;
        return (endTime - startTime) / 1000;
    }

    public String generateKey() {
        String key = "";
        for (int i = 0; i < 10; i++) {
            char c1 = (char) (randomNumber.nextInt(26) + 'a'); // https://stackoverflow.com/questions/2626835/is-there-functionality-to-generate-a-random-character-in-java
            key = key + c1;
        }
        return key;
    }

    public void runTests() {
        // Iterate to test different ratios of PUT and GET requests to the server
        double[] ratioListPUT = { 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2 };
        for (int i = 0; i < ratioListPUT.length; i++) {
            double ratioPUT = ratioListPUT[i];
            totalDurationPUT = 0;
            totalDurationGET = 0;
            numPUTRequests = 0;
            numGETRequests = 0;
            for (int j = 0; j < numRequests; j++) {
                // Do PUT and GET request operations
                double randomRatio = Math.random();
                String key = generateKey();
                if (randomRatio <= ratioPUT) {
                    totalDurationPUT += computeTimeDurationPUT(key, "abcdefghijklmnopqrstuvwxyz");
                } else {
                    totalDurationGET += computeTimeDurationGET(key);
                }
                break;
            }
            double percentagePUT = ratioPUT * 100.0;
            double percentageGET = 100.0 - percentagePUT;
            double latency = 1000.0 * (totalDurationPUT + totalDurationGET) / numRequests;
            System.out.println("The latency of " + percentagePUT + "% PUT requests and " + percentageGET
                    + "% GET requests is: " + latency);
            try {
                this.kvServer.clearStorage();
            } catch (Exception e) {
                System.out.println(e);
            }
            break;
        }
    }

    public static void main(String[] args) {
        new PerformanceTest().runTests();
    }

}
