package testing;

import app_kvServer.KVServer;
import ecs.ECSNode;
import shared.messages.KVMessage.StatusType;
import app_kvServer.PersistantStorage;
import app_kvECS.ECSClient;
import cache.FIFOCache;
import cache.LFUCache;
import cache.LRUCache;

import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.lang.Math;
import logging.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import testing.IndividualClient;
import java.util.ArrayList;
import java.io.File;
import java.nio.file.Files;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;

public class PerformanceTest {
    String hostname = "127.0.0.1";
    int port = 8001;
    private final int numServers;
    private final int numClients;
    private static final String mailDataPath = "/Users/akinowatanabe/Documents/maildir";
    private final String cacheStrategy;
    private final int cacheSize;
    private final boolean nodeTest;

    public PerformanceTest(int numServers, int numClients, String cacheStrategy, int cacheSize, boolean nodeTest) {
        this.numServers = numServers;
        this.numClients = numClients;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.nodeTest = nodeTest;
    }

    public ArrayList<ArrayList<String>> readInMailData(String originalDataPath){
        ArrayList<ArrayList<String>> keyValuePairList = new ArrayList<>();
        try {
            readKeyValuePair(originalDataPath, originalDataPath, keyValuePairList);
        } catch (Exception e) {
            System.out.println(e);
        }
        return keyValuePairList;
    }

    public void readKeyValuePair(String originalDataPath, String newDirPath, ArrayList<ArrayList<String>> keyValuePairList) {
        File newDir = new File(newDirPath);
        File[] fileContent = newDir.listFiles();
        assert fileContent != null;
        for (File f : fileContent) {
            // recursive file read in nested folders
            if (f.isDirectory()) {
                // System.out.println("RECURSIVE CALL FOR readKeyValuePair");
                readKeyValuePair(originalDataPath, f.getPath(), keyValuePairList);
            } else {
                String key = f.getPath().substring(originalDataPath.length()); // full path under maildir
                // System.out.println("key: " + key);
                String value; // contents of the file if exists correctly
                try {
                    // https://howtodoinjava.com/java11/files-readstring-read-file-to-string/
                    value = Files.readString(f.toPath(), Charset.defaultCharset());
                } catch (Exception e) {
                    // System.out.println("readKeyValuePair catch: " + e);
                    continue;
                }
                ArrayList<String> oneKeyValPair = new ArrayList<>();
                oneKeyValPair.add(key);
                oneKeyValPair.add(value);
                keyValuePairList.add(oneKeyValPair);
                if (keyValuePairList.size() % 5000 == 0){
                    System.out.println("Key value pair count " + keyValuePairList.size());
                }
            }
        }
    }

    public void runClientThreads(ArrayList<IndividualClient> clientList) {
        Thread[] threads = new Thread[this.numClients];
        for (int i = 0; i < this.numClients; i++) {
            threads[i] = new Thread(clientList.get(i));
            threads[i].start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public void runTests() {
        int numRequests = 1000;
        ECSClient ecsClient;
        if (!nodeTest) {
            // Mail data handling
            // System.out.println("mailDataPath");
            // System.out.println(mailDataPath);
            List<ArrayList<String>> originalData = this.readInMailData(mailDataPath);
            System.out.println("originalData.size() " + originalData.size()); // 517310
            List<ArrayList<String>> populatingData = originalData.subList(0, numRequests);
            List<ArrayList<String>> clientAllocatingData = originalData.subList(numRequests, numRequests * 2);
            System.out.println("allocated data");
            // Set up ECSClient and add server nodes
            ecsClient = new ECSClient();
            System.out.println("initialized client");
            ecsClient.addNodes(this.numServers, this.cacheStrategy, this.cacheSize);
            System.out.println("Add nodes to client");
            // Populate the storage service with put requests
            ArrayList<IndividualClient> populatingClients = new ArrayList<>();
            int spacing = (populatingData.size() / this.numClients);
            for (int i = 0; i < this.numClients; i++){
                populatingClients.add(new IndividualClient(hostname, port, populatingData.subList(i*spacing, (i+1)*spacing),
                        originalData, numRequests));
            }
            System.out.println("added client threads");
            runClientThreads(populatingClients);
            System.out.println("runClientThreads(populatingClients);");
            // Prepare the clients for latency and throughput computation
            ArrayList<IndividualClient> clients = new ArrayList<>();
            spacing = (clientAllocatingData.size() / numClients);
            for (int i = 0; i < numClients; i++)
                clients.add(new IndividualClient(hostname, port, clientAllocatingData.subList(i*spacing, (i+1)*spacing),
                        originalData, numRequests));
            long startTime = System.currentTimeMillis();
            runClientThreads(clients);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            long latency = 1000 * duration / numRequests;
            System.out.println("The latency (ms) of " + this.cacheStrategy + " cache with the size of " + this.cacheSize
                    + " with " + this.numClients + " clients and " + this.numServers + " servers is: " + latency);
            ecsClient.shutdown();
        } else {
            ecsClient = new ECSClient();
            // Measure time duration for adding nodes
            long startTime = System.currentTimeMillis();
            ecsClient.addNodes(this.numServers, this.cacheStrategy, this.cacheSize);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            long latency = 1000 * duration / numRequests;
            System.out.println("The latency (ms) of adding " + this.numServers + " server nodes with no caching is: "
                    + latency);
            // Measure time duration for removing nodes
            HashMap<String, ECSNode> hashRingMap = ecsClient.getNodes();
            startTime = System.currentTimeMillis();
            ecsClient.removeNodes(hashRingMap.keySet());
            endTime = System.currentTimeMillis();
            duration = endTime - startTime;
            // latency = 1000 * duration / numRequests;
            latency = duration / numRequests;
            System.out.println("The latency (ms) of removing " + this.numServers + " server nodes with no caching is: "
                    + latency);
            ecsClient.shutdown();
        }

    }

    public static void main(String[] args) {
        // Test performance of adding and removing the server nodes
        new PerformanceTest(5, 0, "None", 0, true).runTests();
        new PerformanceTest(20, 0, "None", 0, true).runTests();
        new PerformanceTest(50, 0, "None", 0, true).runTests();
        new PerformanceTest(100, 0, "None", 0, true).runTests();
        // Test performance of using different numbers of clients with constant number of servers
        // No caching
        new PerformanceTest(5, 2, "None", 0, false).runTests();
        new PerformanceTest(5, 10, "None", 0, false).runTests();
        new PerformanceTest(5, 20, "None", 0, false).runTests();
        // Test performance of using different numbers of servers with constant number of clients
        // No caching
        new PerformanceTest(1, 10, "None", 0, false).runTests();
        new PerformanceTest(10, 10, "None", 0, false).runTests();
        new PerformanceTest(20, 10, "None", 0, false).runTests();
        // Test performance of using different types of cache strategy
        // Same number of servers and clients and cache size
        new PerformanceTest(5, 10, "FIFO", 50, false).runTests();
        new PerformanceTest(5, 10, "LFU", 50, false).runTests();
        new PerformanceTest(5, 10, "LRU", 50, false).runTests();
        // Test performance of using different cache size
        // Same number of servers and clients and cache strategy (FIFO)
        new PerformanceTest(5, 10, "FIFO", 20, false).runTests();
        new PerformanceTest(5, 10, "FIFO", 100, false).runTests();
        new PerformanceTest(5, 10, "FIFO", 200, false).runTests();
    }

}
