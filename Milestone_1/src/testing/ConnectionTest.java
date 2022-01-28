package testing;

import java.net.UnknownHostException;

import client.KVStore;
import app_kvServer.KVServer;

import junit.framework.TestCase;
public class ConnectionTest extends TestCase {

	private KVServer kvServer;

	public void setUp() {
		kvServer = new KVServer(50000, 10, "NONE");
		Thread testThread = new Thread(kvServer);
		testThread.start();
	}

	public void tearDown(){
		kvServer.clearCache();
		kvServer.clearStorage();
		try{
			kvServer.close();
		} catch (NullPointerException e){
			// raises error when tearing down after illegal port
			int tmp = 0;
		}
	}

	public void testConnectionSuccess() {

		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		// kill client to avoid ServerSocket null error
		kvClient.disconnect();
		assertNull(ex);
	}

	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		// kill client to avoid ServerSocket null error
		kvClient.disconnect();
		assertTrue(ex instanceof UnknownHostException);
	}

	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		// kill client to avoid ServerSocker null error
		kvClient.disconnect();
		assertTrue(ex instanceof IllegalArgumentException);
	}

}
