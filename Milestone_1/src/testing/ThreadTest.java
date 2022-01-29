package testing;
import client.KVStore;
import app_kvServer.KVServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ThreadTest {

    private KVServer kvServer;
    private KVStore kvStore;
    private static final int NUM_THREADS = 10;
    KVStore users[] = new KVStore[NUM_THREADS];
    Thread threads[] = new Thread[NUM_THREADS];
    int port = 8085;
    
    public void testThreads(){
        kvServer = new KVServer(port, 0, "");

        for(int i=0; i < NUM_THREADS; i++){
            // create store
            users[i] = new KVStore("localhost", port, i, NUM_THREADS);

            // run the put + update + get requests
            threads[i] = new Thread(users[i]);

            // start the thread
			System.out.println("SANDAKJFHIRHGU: ");
            threads[i].start();

        }

    }

    public static void main(String[] args) {
        new ThreadTest().testThreads();
    }
}
