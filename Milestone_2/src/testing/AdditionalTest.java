package testing;

import java.nio.file.*;
import java.io.File;
import java.beans.Transient;
import java.math.BigInteger;
import junit.framework.TestCase;
import org.junit.Test;

import client.KVStore;

import app_kvServer.PersistantStorage;

import shared.messages.KVMessage.StatusType;
import shared.Utils;

public class AdditionalTest extends TestCase {

	private PersistantStorage persistantStorage;
	private Utils utils = new Utils();

	public void setUp() {
		try {
			persistantStorage = new PersistantStorage("50001");
			persistantStorage.clearStorage();
		} catch (Exception e) {
			System.out.println("Failed to set up persistant storage");
			System.out.println(e);
		}
	}

	public void tearDown() {
		persistantStorage.deleteStorage();
	}

	@Test
	public void testFileCreated() {
		Path path = Paths.get("./storage/50001_storage.txt");
		assertTrue(Files.exists(path));
	}

	@Test
	public void testFileDeleted() {
		Path path;

		path = Paths.get("./storage/50001_storage.txt");
		assertTrue(Files.exists(path));

		persistantStorage.deleteStorage();

		path = Paths.get("./storage/50001_storage.txt");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testFileCleared() {
		String key = "foo", value = "bar";
		Path path;
		File f;
		String pathToFile = "./storage/50001_storage.txt";
		Exception ex = null;

		path = Paths.get(pathToFile);
		assertTrue(Files.exists(path));
		f = new File(pathToFile);
		assertTrue(f.length() == 0);

		try {
			persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && f.length() > 0);

		persistantStorage.clearStorage();
		assertTrue(f.length() == 0);

		path = Paths.get(pathToFile);
		assertTrue(Files.exists(path));
	}

	@Test
	public void testPutSuccess() {
		String key = "foo", value = "bar";
		StatusType status = null;
		Exception ex = null;

		try {
			status = persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_SUCCESS);
	}

	@Test
	public void testPutUpdate() {
		String key = "foo", value = "bar";
		StatusType status = null;
		Exception ex = null;

		try {
			persistantStorage.put(key, "bar1");
			status = persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_UPDATE);
	}

	@Test
	public void testDeleteSuccess() {
		String key = "foo", value = "";
		StatusType status = null;
		Exception ex = null;

		try {
			persistantStorage.put(key, "bar1");
			status = persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.DELETE_SUCCESS);
	}

	@Test
	public void testDeleteError() {
		String key = "foo", value = "";
		StatusType status = null;
		Exception ex = null;

		try {
			status = persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.DELETE_ERROR);
	}

	@Test
	public void testPutError() {
		String key = "foo", value = "bar";
		StatusType status = null;
		Exception ex = null;

		try {
			// Remove the file so there's nothing to write to
			persistantStorage.deleteStorage();
			status = persistantStorage.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_ERROR);
	}

	@Test
	public void testGetSuccessful() {
		String key = "foo", value = "bar";
		String getValue = "";
		Exception ex = null;

		try {
			persistantStorage.put(key, value);
			getValue = persistantStorage.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue.equals(value));
	}

	@Test
	public void testGetError() {
		String key = "foo";
		String getValue = "";
		Exception ex = null;

		try {
			getValue = persistantStorage.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue == null);
	}

	@Test
	public void testGetThrowException() {
		String key = "foo";
		String getValue = "";
		Exception ex = null;

		try {
			// Remove the file so there's nothing to read from
			persistantStorage.deleteStorage();
			getValue = persistantStorage.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue == null);
	}

	@Test
	public void testInStorage() {
		String key = "foo", value = "bar";
		Exception ex = null;
		boolean inStorage = false;

		try {
			persistantStorage.put(key, value);
			inStorage = persistantStorage.inStorage(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && inStorage);
	}

	@Test
	public void testNotInStorage() {
		String key = "foo";
		Exception ex = null;
		boolean inStorage = true;

		try {
			inStorage = persistantStorage.inStorage(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && !inStorage);
	}

	@Test
	public void testPutManySuccess() {
		String key = "put_many";
		String value = "{'status':'NO_STATUS','key':'lisa','value':'l'}\n{'status':'NO_STATUS','key':'akino','value':'w'}\n";
		Exception ex = null;
		boolean inStorage = true;

		try {
			inStorage = persistantStorage.appendToStorage(value);

		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_SUCCESS);

		String getValue;
		try {
			getValue = persistantStorage.get("lisa");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue.equals("l"));
	}

	@Test
	public void testPutManyError() {
		String key = "foo";
		String value = "{'status':'NO_STATUS','key':'lisa','value':'l'}\n{'status':'NO_STATUS','key':'akino','value':'w'}\n";
		StatusType status = null;
		Exception ex = null;

		try {
			// Remove the file so there's nothing to write to
			persistantStorage.deleteStorage();
			status = persistantStorage.appendToStorage(value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_ERROR);
	}

	@Test
	public void testGetDataInRange() {
		String output;
		Exception ex = null;
		String getValue;

		persistantStorage.put("key1", "value1");
		persistantStorage.put("key2", "value1");
		persistantStorage.put("key3", "value1");
		persistantStorage.put("key4", "value1");

		String getValue;
		try {
			getValue = persistantStorage.get("key1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue.equals("value1"));

		BigInteger hash = utils.getHash("key1");
		BigInteger endHash = hash.add(new BigInteger("1"));

		try {
			output = persistantStorage.getDataInRange(hash, endHash, false);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && output.equals("{'status':'NO_STATUS','key':'key1','value':'value1'}\n"));

		try {
			getValue = persistantStorage.get("key1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue == null);
	}

	@Test
	public void testGetDataInRangeOnDeath() {
		String output;
		Exception ex = null;
		String getValue;

		persistantStorage.put("key1", "value1");
		persistantStorage.put("key2", "value1");
		persistantStorage.put("key3", "value1");
		persistantStorage.put("key4", "value1");

		BigInteger hash = utils.getHash("key1");
		BigInteger endHash = hash.add(new BigInteger("1"));

		try {
			output = persistantStorage.getDataInRange(hash, endHash, true);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && output.equals(
				"{'status':'NO_STATUS','key':'key1','value':'value1'}\n{'status':'NO_STATUS','key':'key2','value':'value2'}\n{'status':'NO_STATUS','key':'key3','value':'value3'}\n{'status':'NO_STATUS','key':'key4','value':'value4'}\n"));

		assertTrue(persistantStorage.isEmpty() == true);
	}

}
