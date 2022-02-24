package shared.messages;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;

import com.google.gson.Gson;

import ecs.ECSNode;

public class ECSMessage implements Serializable {
    
    public String name;
    public BigInteger hash;
    public BigInteger endHash; // non-inclusive
    public int port;
    public MessageType status;
    public HashMap<String, BigInteger> order;
    public String data;

    private ECSNode receiver;


    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    public enum MessageType {
      START, // start KVServer,
      STOP,  // stop KVServer
      SHUTDOWN,
      LOCKED,
      UNLOCK, // unlock
      MOVE_DATA,
      CLEAR_STORAGE, // clears persistant storage
      DELETE_STORAGE, // deletes persistant storage

    }

    public ECSMessage() {
    }

    public MessageType getStatus() {
        return null;
    }

    public ECSNode getReceiver() {
        return this.receiver;
    }
}
