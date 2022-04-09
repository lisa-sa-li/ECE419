package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;

public class RecoveryTest extends TestCase {

    private KVStore kvStore;

    public void setUp() {
        kvStore = new KVStore("localhost", 50000);
        try {
            kvStore.connect();
        } catch (Exception e) {
            // logger.info("FAILED TO CONNECT CLIENT");
            System.out.println(e);

        }
    }

    public void tearDown() {
        kvStore.disconnect();
    }

    @Test
    public void testRecover() {
        String key = "key1", value = "val1";
        JSONMessage response = null;
        Exception ex = null;

        try {
            response = kvStore.put(key, value);
            response = kvStore.put(key, "");
            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
        } catch (Exception e) {
            ex_final = e;
        }

        assertTrue(ex_final == null && final_response.getStatus() == StatusType.RECOVER_SUCCESS);
    }

    @Test
    public void testMultipleKeys() {
        String key = "key2", value = "val2";
        String key2 = "key3", value2 = "val3";
        JSONMessage response = null;
        Exception ex = null;
        JSONMessage final_response = null;
        Exception ex_final = null;
        JSONMessage final_response2 = null;

        try {
            response = kvStore.put(key, value);
            response = kvStore.put(key2, value2);
            response = kvStore.put(key, "");
            response = kvStore.put(key2, "");
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
            final_response2 = kvStore.recover(key2);
        } catch (Exception e) {
            ex_final = e;
        }
        assertTrue(ex_final == null && final_response.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(ex_final == null && final_response2.getStatus() == StatusType.RECOVER_SUCCESS);
    }

    @Test
    public void testRecoverNonExistentKey() {
        String key = "key4", value = "val4";
        JSONMessage response = null;
        Exception ex = null;

        try {
            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverTimeDuration() {
        String key = "key5", value = "val5";
        JSONMessage response = null;
        Exception ex = null;
        JSONMessage final_response = null;
        Exception final_ex = null;

        try {
            response = kvStore.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        try {
            TimeUnit.MINUTES.sleep(1);
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
        } catch (Exception e) {
            final_ex = e;
        }

        assertTrue(final_ex == null && final_response.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverTimeDurationMultiple() {
        String key = "key5", value = "val5";
        String key2 = "key6", value2 = "val6";
        JSONMessage response = null;
        Exception ex = null;
        JSONMessage final_response = null;
        JSONMessage final_response2 = null;
        Exception final_ex = null;

        try {
            response = kvStore.put(key, value);
            response = kvStore.put(key2, value2);
        } catch (Exception e) {
            ex = e;
        }

        try {
            TimeUnit.MINUTES.sleep(1);
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
            final_response2 = kvStore.recover(key2);
        } catch (Exception e) {
            final_ex = e;
        }

        assertTrue(final_ex == null && final_response.getStatus() == StatusType.RECOVER_ERROR);
        assertTrue(final_response2.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverwriteWithUpdte() {
        String key = "key7", value = "val7", newVal = "val8";
        JSONMessage response = null;
        Exception ex = null;
        JSONMessage final_response = null;
        JSONMessage final_response2 = null;
        Exception final_ex = null;

        try {
            response = kvStore.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        try {
            TimeUnit.MINUTES.sleep(1);
        } catch (Exception e) {
            ex = e;
        }

        try {
            response = kvStore.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);

        try {
            response = kvStore.put(key, newVal);
        } catch (Exception e) {
            ex = e;
        }

        try {
            response = kvStore.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS && response.getValue() == "val8");

        try {
            final_response = kvStore.recover(key);
        } catch (Exception e) {
            final_ex = e;
        }

        assertTrue(final_ex == null && final_response.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(final_response.getValue() == "val8");
    }
    
}
