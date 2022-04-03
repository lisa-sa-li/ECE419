package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logging.LogSetup;
import java.io.File;
import org.apache.log4j.Logger;

public class AllTests {
	private static Logger logger = Logger.getRootLogger();

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.DEBUG);
			KVServer kvServer = new KVServer(50000, 3, "FIFO", true);
			kvServer.clearStorage();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		// clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		clientSuite.addTestSuite(FIFOCacheTest.class);
		clientSuite.addTestSuite(LRUCacheTest.class);
		clientSuite.addTestSuite(LFUCacheTest.class);
		clientSuite.addTestSuite(ECSInteractionTest.class);
		// clientSuite.addTestSuite(ReplicaTest.class);
		return clientSuite;
	}

}
