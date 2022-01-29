package testing;

import org.junit.Test;

import app_kvServer.PersistantStorage;
import junit.framework.TestCase;
import shared.messages.KVMessage.StatusType;
import java.nio.file.*;

public class AdditionalTest extends TestCase {

	private PersistantStorage persistantStorage;

	public void setUp() {
		try {
			persistantStorage = new PersistantStorage("50001");
		} catch (Exception e) {
			System.out.println("Failed to set up persistant storage");
			System.out.println(e);
		}
	}

	public void tearDown() {
		persistantStorage.clearStorage();
	}

	@Test
	public void testFileCreated() {
		Path path = Paths.get("./storage/50001_storage.txt");
		assertTrue(Files.exists(path));
	}

	@Test
	public void testFileCleared() {
		Path path;

		path = Paths.get("./storage/50001_storage.txt");
		assertTrue(Files.exists(path));

		persistantStorage.clearStorage();

		path = Paths.get("./storage/50001_storage.txt");
		assertFalse(Files.exists(path));
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
			persistantStorage.clearStorage();
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
			persistantStorage.clearStorage();
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
}
