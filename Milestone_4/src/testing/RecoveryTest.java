package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import java.util.concurrent.TimeUnit;
import java.util.*;
import app_kvECS.ECSClient;

public class RecoveryTest extends TestCase {

    private KVStore kvStore;
    private ECSClient ecs;

    public void setUp() {
        ecs = new ECSClient("./test_servers.cfg");
        ecs.addNode("FIFO", 3);

        try {
            TimeUnit.SECONDS.sleep(3);
            ecs.start();
            ArrayList<Integer> ports = ecs.getCurrentPorts();
            kvStore = new KVStore("localhost", ports.get(0));
            kvStore.connect();
        } catch (Exception e) {
            // logger.info("FAILED TO CONNECT CLIENT");
            System.out.println(e);

        }
    }

    public void tearDown() {

        kvStore.disconnect();
        ecs.shutdown();
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (Exception e) {

        }
    }

    @Test
    public void testRecover() {
        String key = "key0", value = "val0";
        JSONMessage response = null;
        JSONMessage final_response = null;
        Exception ex = null;
        Exception ex_final = null;

        try {
            response = kvStore.put(key, value);
            response = kvStore.put(key, "");
        } catch (Exception e) {
            ex = e;
        }

        try {
            TimeUnit.SECONDS.sleep(60);
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
    public void testCannotRecover() {
        String key = "key1", value = "val1";
        JSONMessage response = null;
        JSONMessage final_response = null;
        Exception ex = null;
        Exception ex_final = null;

        try {
            response = kvStore.put(key, value);
            // System.out.println("response first: " + response.getStatus());
            // response = kvStore.put(key, "");
            // System.out.println("response second: " + response.getStatus());
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
            // System.out.println("final_response: " + final_response.getStatus() +
            // final_response.getKey() + final_response.getValue());
        } catch (Exception e) {
            ex_final = e;
        }

        assertTrue(ex_final == null && final_response.getStatus() == StatusType.RECOVER_ERROR);
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
            TimeUnit.SECONDS.sleep(60);
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
        assertTrue(final_response.getValue().equals("val2"));
        assertTrue(final_response2.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(final_response2.getValue().equals("val3"));
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
        System.out.println("response 1: " + response.getStatus());
        try {
            TimeUnit.SECONDS.sleep(90);
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
        } catch (Exception e) {
            final_ex = e;
        }
        System.out.println("final_response 2: " + final_response.getStatus());
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
            TimeUnit.SECONDS.sleep(60);
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
            // System.out.println("response 1: " + response.getStatus());
        } catch (Exception e) {
            ex = e;
        }

        try {
            TimeUnit.SECONDS.sleep(90);
        } catch (Exception e) {
            ex = e;
        }

        /*try {
            response = kvStore.get(key);
            System.out.println("response 2: " + response.getStatus());
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
        */
        try {
            response = kvStore.put(key, newVal);
            // System.out.println("response 3: " + response.getStatus());
        } catch (Exception e) {
            ex = e;
        }

        try {
            final_response = kvStore.recover(key);
            // System.out.println("response 5: " + final_response.getStatus());
        } catch (Exception e) {
            final_ex = e;
        }

        assertTrue(final_ex == null && final_response.getStatus() == StatusType.RECOVER_ERROR);
        // assertTrue(final_response.getValue().equals("val8"));

        try {
            response = kvStore.get(key);
            // System.out.println("response 4: " + response.getStatus());
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS &&
                response.getValue().equals("val8"));
    }

}
