package app_kvServer;

import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CyclicBarrier;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.io.OutputStream;

import org.apache.log4j.*;
import org.apache.log4j.Logger;
import logging.ServerLogSetup;

import ecs.ECSNode;
import shared.messages.JSONMessage;
import shared.messages.KVMessage.StatusType;
import app_kvServer.PersistantStorage;

// public class ControllerSender implements Runnable {
public class ControllerSender {
    private static Logger logger = Logger.getRootLogger();

    private ECSNode replicate;
    private KVServer kvServer;
    private CyclicBarrier barrier;
    private String msg;
    private String action;

    public ControllerSender(ECSNode replicate, KVServer kvServer, CyclicBarrier barrier, String msg, String action) {
        // needs to know replicate info
        this.replicate = replicate;
        this.kvServer = kvServer;
        this.barrier = barrier;
        this.msg = msg;
        this.action = action;
    }

    // @Override
    // public void run() {
    public void sendMsg() {
        String hostOfReceiver = replicate.getNodeHost();
        String nameOfReceiver = replicate.getNodeName();

        try {
            Socket socket = new Socket(hostOfReceiver, replicate.getReplicateReceiverPort());
            OutputStream output = socket.getOutputStream();

            JSONMessage json = new JSONMessage();
            switch (action) {
                case "init":
                    json.setMessage(StatusType.INIT_REPLICATE_DATA.name(), "put_many", msg);
                    break;
                case "update":
                    json.setMessage(StatusType.UPDATE_REPLICATE_DATA.name(), "update", msg);
                    break;
                case "delete":
                    json.setMessage(StatusType.DELETE_REPLICATE_DATA.name(), "delete", msg);
                    break;
            }
            byte[] jsonBytes = json.getJSONByte();

            output.write(jsonBytes, 0, jsonBytes.length);
            output.flush();
            output.close();
            socket.close();
        } catch (SocketTimeoutException s){
			logger.info("Socket timeout in Controller sender: retrying " + s);
		} catch (Exception e) {
            logger.error("Unable to send data to replicant " + nameOfReceiver + ", " + e);
        }
    }

}
