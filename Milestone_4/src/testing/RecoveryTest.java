package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import java.util.concurrent.TimeUnit;
import java.util.*;
import app_kvECS.ECSClient;
import app_kvServer.PersistantStorage;

public class RecoveryTest extends TestCase {

    private KVStore kvStore;
    private ECSClient ecs;
    private PersistantStorage ps;

    public void setUp() {
        ps = new PersistantStorage("50001");
        ecs = new ECSClient("./test_servers.cfg");
        ecs.addNode("FIFO", 3);

        try {
            TimeUnit.SECONDS.sleep(10);
            ArrayList<Integer> ports = ecs.getCurrentPorts();
            ecs.start();
            TimeUnit.SECONDS.sleep(5);
            kvStore = new KVStore("localhost", ports.get(0));
            kvStore.connect();

            ecs.clearTrash();
        } catch (Exception e) {
            System.out.println(e);

        }
    }

    public void tearDown() {
        kvStore.disconnect();
        ecs.shutdown();
        try {
            TimeUnit.SECONDS.sleep(10);
            ps.clearGlobalStorage();
            ps.deleteStorage();
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception e) {
        }
    }

    @Test
    public void testRecover() {
        String key = "key0", value = "val0";
        JSONMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, value);
            kvStore.put(key, "");
            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testRecover: " + response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_SUCCESS);
    }

    @Test
    public void testCannotRecover() {
        String key = "key1", value = "val1";
        JSONMessage response = null;
        Exception ex = null;

        try {
            response = kvStore.put(key, value);
            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testCannotRecover: " + response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testMultipleKeys() {
        String key = "key2", value = "val2";
        String key2 = "key3", value2 = "val3";
        Exception ex = null;
        JSONMessage response = null, response2 = null;

        try {
            kvStore.put(key, value);
            kvStore.put(key2, value2);
            kvStore.put(key, "");
            kvStore.put(key2, "");

            response = kvStore.recover(key);
            response2 = kvStore.recover(key2);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testMultipleKeys1: " + response.getStatus());
        // System.out.println("testMultipleKeys2: " + response2.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(response.getValue().equals("val2"));
        assertTrue(response2.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(response2.getValue().equals("val3"));
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

        // System.out.println("testRecoverNonExistentKey: " + response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverTimeDuration() {
        String key = "key5", value = "val5";
        JSONMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, value);
            kvStore.put(key, "");
            TimeUnit.SECONDS.sleep(90);

            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testRecoverOverTimeDuration: " + response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverTimeDurationMultiple() {
        String key = "key5", value = "val5";
        String key2 = "key6", value2 = "val6";
        Exception ex = null;
        JSONMessage response = null, response2 = null;

        try {
            kvStore.put(key, value);
            kvStore.put(key2, value2);
            kvStore.put(key, "");
            kvStore.put(key2, "");

            TimeUnit.SECONDS.sleep(90);

            response = kvStore.recover(key);
            response2 = kvStore.recover(key2);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testRecoverOverTimeDurationMultiple1: " +
        // response.getStatus());
        // System.out.println("testRecoverOverTimeDurationMultiple2: " +
        // response2.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_ERROR);
        assertTrue(response2.getStatus() == StatusType.RECOVER_ERROR);
    }

    @Test
    public void testRecoverOverwriteWithUpdate() {
        String key = "key7", value = "val7", newVal = "val8";
        JSONMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, value);
            kvStore.put(key, "");

            kvStore.put(key, newVal);

            response = kvStore.recover(key);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testRecoverOverwriteWithUpdate1: " +
        // response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.RECOVER_SUCCESS);
        assertTrue(response.getValue().equals(value));

        try {
            response = kvStore.get(key);
        } catch (Exception e) {
            ex = e;
        }

        // System.out.println("testRecoverOverwriteWithUpdate2: " +
        // response.getStatus());
        assertTrue(ex == null && response.getStatus() == StatusType.GET_SUCCESS &&
                response.getValue().equals(value));
    }

}
