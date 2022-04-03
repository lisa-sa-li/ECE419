package testing;

import java.nio.file.*;
import java.io.File;
import java.math.BigInteger;
import junit.framework.TestCase;
import org.junit.Test;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import app_kvServer.PersistantStorage;

import shared.messages.KVMessage.StatusType;
import shared.Utils;

public class AdditionalTest extends TestCase {

	private PersistantStorage persistantStorage;
	private Utils utils = new Utils();
	private String GLOBAL_STORAGE_PATH = "./storage/global_storage_test.txt";

	public void setUp() {
		try {
			persistantStorage = new PersistantStorage("50001", GLOBAL_STORAGE_PATH);
			persistantStorage.clearStorage();
			clearFile(GLOBAL_STORAGE_PATH);
		} catch (Exception e) {
			System.out.println("Failed to set up persistant storage");
			System.out.println(e);
		}
	}

	public void tearDown() {
		persistantStorage.deleteStorage();
	}

	public String getAllFromFile(String pathToFile) {
		// Appends kv-pairs to the end of the storage file
		try {
			BufferedReader file = new BufferedReader(new FileReader(pathToFile));
			StringBuffer buffer = new StringBuffer();
			String line;

			while ((line = file.readLine()) != null) {
				buffer.append(line);
				buffer.append('\n');
			}

			file.close();
			return buffer.toString();
		} catch (Exception e) {
		}
		return "";
	}

	public void clearFile(String pathToFile) {
		try {
			PrintWriter writer = new PrintWriter(pathToFile);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
		} catch (Exception e) {
		}
	}

	public void writeToStorage(String pathToFile, String str) {
		// Appends kv-pairs to the end of the storage file
		try {
			// BufferedReader file = new BufferedReader(new FileReader(pathToFile));
			// StringBuffer inputBuffer = new StringBuffer();
			// String line;

			// inputBuffer.append(str);
			// file.close();
			// Overwrite file with the string buffer data
			FileOutputStream fileOut = new FileOutputStream(pathToFile);
			fileOut.write(str.getBytes());
			fileOut.close();

			System.out.println("Completed 'put_many' operation into storage server");
		} catch (Exception e) {
			System.out.println("Problem reading file to put_many.");
		}
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
		StatusType status = null;
		String getValue = "";

		try {
			status = persistantStorage.appendToStorage(value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && status == StatusType.PUT_SUCCESS);

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
		String output = "", getValue = "";
		Exception ex = null;

		try {
			persistantStorage.put("key1", "value1");
			persistantStorage.put("key2", "value1");
			persistantStorage.put("key3", "value1");
			persistantStorage.put("key4", "value1");
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

		assertTrue(ex == null &&
				output.equals("{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n"));

		try {
			getValue = persistantStorage.get("key1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && getValue == null);
	}

	@Test
	public void testGetDataInRangeOnDeath() {
		String output = "";
		Exception ex = null;

		BigInteger hash = utils.getHash("key1");
		BigInteger endHash = hash.add(new BigInteger("1"));

		try {
			persistantStorage.put("key1", "value1");
			persistantStorage.put("key2", "value2");
			persistantStorage.put("key3", "value3");
			persistantStorage.put("key4", "value4");
			output = persistantStorage.getDataInRange(hash, endHash, true);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && output.equals(
				"{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key2\",\"value\":\"value2\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key3\",\"value\":\"value3\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key4\",\"value\":\"value4\"}\n"));

		assertTrue(persistantStorage.isEmpty() == true);
	}

	@Test
	public void testGetAllFromStorage() {
		String output = "";
		Exception ex = null;

		try {
			persistantStorage.put("key1", "value1");
			persistantStorage.put("key2", "value2");
			persistantStorage.put("key3", "value3");
			persistantStorage.put("key4", "value4");
			output = persistantStorage.getAllFromStorage();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && output.equals(
				"{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key2\",\"value\":\"value2\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key3\",\"value\":\"value3\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key4\",\"value\":\"value4\"}\n"));

		assertTrue(persistantStorage.isEmpty() == false);
	}

	@Test
	public void testMoveToGlobalStorage() {
		String output = "";
		Exception ex = null;

		try {
			System.out.println("testMoveToGlobalStorage");
			persistantStorage.put("key1", "value1");
			System.out.println("Put 1");
			persistantStorage.put("key2", "value2");
			System.out.println("Put 2");
			persistantStorage.moveToGlobalStorage();
			System.out.println("moved to");
		} catch (Exception e) {
			ex = e;
		}
		System.out.println("getAllFromFile(GLOBAL_STORAGE_PATH): " + getAllFromFile(GLOBAL_STORAGE_PATH));
		assertTrue(ex == null && getAllFromFile(GLOBAL_STORAGE_PATH).equals(
				"{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key2\",\"value\":\"value2\"}\n"));
		System.out.println("persistantStorage.isEmpty(): " + persistantStorage.isEmpty());
		assertTrue(persistantStorage.isEmpty() == true);
	}

	@Test
	public void testGetFromGlobalStorage() {
		String output = "";
		Exception ex = null;

		try {
			System.out.println("testGetFromGlobalStorage");
			writeToStorage(GLOBAL_STORAGE_PATH,
					"{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key2\",\"value\":\"value2\"}\n");
			System.out.println("writeToStorage");
			persistantStorage.getFromGlobalStorage();
		} catch (Exception e) {
			ex = e;
		}
		System.out.println("persistantStorage.getAllFromStorage(): " + persistantStorage.getAllFromStorage());
		assertTrue(ex == null && persistantStorage.getAllFromStorage().equals(
				"{\"status\":\"NO_STATUS\",\"key\":\"key1\",\"value\":\"value1\"}\n{\"status\":\"NO_STATUS\",\"key\":\"key2\",\"value\":\"value2\"}\n"));
		System.out.println("persistantStorage.isEmpty(): " + persistantStorage.isEmpty());
		assertTrue(persistantStorage.isEmpty() == false);
	}

}
