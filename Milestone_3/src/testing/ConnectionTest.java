package testing;

import java.net.UnknownHostException;

import client.KVStore;
import app_kvServer.KVServer;

import junit.framework.TestCase;

public class ConnectionTest extends TestCase {

	private KVServer kvServer;
	private Thread testThread;

	public void testConnectionSuccess() {
		Exception ex = null;
		KVStore kvStore = new KVStore("localhost", 50000);

		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}

	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvStore = new KVStore("unknown", 50000);

		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}

	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvStore = new KVStore("localhost", 123456789);

		try {
			kvStore.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof IllegalArgumentException);
	}

}
