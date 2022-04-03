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
import java.util.concurrent.TimeUnit;
import java.nio.file.Paths;

public class PerformanceTest {
    String hostname = "127.0.0.1";
    int port;
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

    public ArrayList<ArrayList<String>> readInMailData(String originalDataPath) {
        ArrayList<ArrayList<String>> keyValuePairList = new ArrayList<>();
        try {
            readKeyValuePair(originalDataPath, originalDataPath, keyValuePairList);
        } catch (Exception e) {
            System.out.println(e);
        }
        return keyValuePairList;
    }

    public void readKeyValuePair(String originalDataPath, String newDirPath,
            ArrayList<ArrayList<String>> keyValuePairList) {
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
                    // value = Files.readString(f.toPath(), Charset.defaultCharset());
                    value = new String(Files.readAllBytes(Paths.get(String.valueOf(f.toPath()))));
                } catch (Exception e) {
                    // System.out.println("readKeyValuePair catch: " + e);
                    continue;
                }
                ArrayList<String> oneKeyValPair = new ArrayList<>();
                oneKeyValPair.add(key);
                oneKeyValPair.add(value);
                keyValuePairList.add(oneKeyValPair);
            }
        }
    }

    public void runClientThreads(ArrayList<IndividualClient> clientList) {
        Thread[] threads = new Thread[this.numClients];
        for (int i = 0; i < this.numClients; i++) {
            threads[i] = new Thread(clientList.get(i));
            // System.out.println("created thread " + i);
            threads[i].start();
            // System.out.println("started thread " + i);
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        for (Thread thread : threads) {
            try {
                thread.join(60000);
                System.out.println("joined successfully");
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (Exception e) {
                    System.out.println(e);
                }
            } catch (Exception e) {
                System.out.println("Error: " + e);
            }
        }
    }

    public void runTests() {
        int numRequests = 8;
        ECSClient ecsClient;
        if (!nodeTest) {
            // Mail data handling
            List<ArrayList<String>> originalData = this.readInMailData(mailDataPath);
            // System.out.println("originalData.size() " + originalData.size()); // 517310
            List<ArrayList<String>> populatingData = originalData.subList(0, numRequests);
            List<ArrayList<String>> clientAllocatingData = originalData.subList(numRequests, numRequests * 2);
            // System.out.println("allocated data");
            // Set up ECSClient and add server nodes
            ecsClient = new ECSClient("./servers.cfg");
            // System.out.println("initialized client");
            ecsClient.addNodes(this.numServers, this.cacheStrategy, this.cacheSize);
            ecsClient.start();
            try {
                TimeUnit.SECONDS.sleep(this.numServers * 5);
            } catch (Exception e) {
                System.out.println(e);
            }
            ArrayList<Integer> currentOpenPorts = ecsClient.getCurrentPorts();
            this.port = currentOpenPorts.get(0);
            System.out.println("Add nodes to client to port: " + port);
            // Populate the storage service with put requests
            ArrayList<IndividualClient> clients = new ArrayList<>();
            int spacing = (populatingData.size() / this.numClients);
            for (int i = 0; i < this.numClients; i++) {
                clients.add(new IndividualClient(hostname, port, populatingData.subList(i * spacing, (i + 1) * spacing),
                        originalData, numRequests, true));
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            // System.out.println("added client threads");
            runClientThreads(clients);
            // System.out.println("runClientThreads(populatingClients);");
            // Prepare the clients for latency and throughput computation
            clients = new ArrayList<>();
            spacing = (clientAllocatingData.size() / numClients);
            for (int i = 0; i < this.numClients; i++) {
                clients.add(new IndividualClient(hostname, port,
                        clientAllocatingData.subList(i * spacing, (i + 1) * spacing),
                        populatingData, numRequests, false));
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            System.out.println("added client threads for not populating");
            long startTime = System.currentTimeMillis();
            runClientThreads(clients);
            long endTime = System.currentTimeMillis();
            // System.out.println("runClientThreads(clients);");
            long duration = endTime - startTime;
            long latency = 1000 * duration / numRequests;
            System.out.println("The latency (ms) of " + this.cacheStrategy + " cache with the size of " + this.cacheSize
                    + " with " + this.numClients + " clients and " + this.numServers + " servers is: " + latency);
            double totalBytes = 0;
            for (int i = 0; i < numClients; i++)
                totalBytes += clients.get(i).getTotalBytes();
            System.out.println("totalBytes and duration: " + totalBytes + " : " + duration);
            double throughput = totalBytes / (duration);
            System.out.println("The throughput of " + this.cacheStrategy + " cache with the size of " + this.cacheSize
                    + " with " + this.numClients + " clients and " + this.numServers + " servers is: " + throughput);
            ecsClient.shutdown();
        } else {
            ecsClient = new ECSClient("./servers.cfg");
            // Measure time duration for adding nodes
            long startTime = System.currentTimeMillis();
            ecsClient.addNodes(this.numServers, this.cacheStrategy, this.cacheSize);
            long endTime = System.currentTimeMillis();
            long duration = (endTime - startTime);
            System.out.println(
                    "The time duration (ms) of adding " + this.numServers + " server nodes with no caching is: "
                            + duration);
            // Measure time duration for removing nodes
            HashMap<String, ECSNode> hashRingMap = ecsClient.getNodes();
            startTime = System.currentTimeMillis();
            ecsClient.removeNodes(hashRingMap.keySet());
            endTime = System.currentTimeMillis();
            duration = (endTime - startTime);
            System.out.println("The duration (ms) of removing " + this.numServers + " server nodes with no caching is: "
                    + duration);
            ecsClient.shutdown();
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception e) {
                System.out.println(e);
            }
        }

    }

    public static void main(String[] args) {
        // Test performance of adding and removing the server nodes
        new PerformanceTest(1, 0, "None", 0, true).runTests();
        new PerformanceTest(3, 0, "None", 0, true).runTests();
        new PerformanceTest(5, 0, "None", 0, true).runTests();
        new PerformanceTest(7, 0, "None", 0, true).runTests();
        new PerformanceTest(10, 0, "None", 0, true).runTests();
        // Test performance of using different numbers of servers with constant number
        // of clients
        // No caching
        new PerformanceTest(3, 2, "None", 0, false).runTests();
        new PerformanceTest(2, 2, "None", 0, false).runTests();
        new PerformanceTest(1, 2, "None", 0, false).runTests();
        // Test performance of using different numbers of clients with constant number
        // of servers
        // No caching
        new PerformanceTest(2, 2, "None", 0, false).runTests();
        new PerformanceTest(2, 4, "None", 0, false).runTests();
        // new PerformanceTest(2, 1, "None", 0, false).runTests();
        // Test performance of using different types of cache strategy
        // Same number of servers and clients and cache size
        new PerformanceTest(2, 1, "FIFO", 8, false).runTests();
        new PerformanceTest(2, 1, "LFU", 8, false).runTests();
        new PerformanceTest(2, 1, "LRU", 8, false).runTests();
        // Test performance of using different cache size
        // Same number of servers and clients and cache strategy (FIFO)
        // new PerformanceTest(2, 1, "FIFO", 20, false).runTests();
        // new PerformanceTest(2, 1, "FIFO", 10, false).runTests();
        new PerformanceTest(2, 1, "FIFO", 3, false).runTests();
    }

}
