package shared.messages;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;

import ecs.ECSNode;

import com.google.gson.Gson;

public class Metadata implements Serializable {
    
    public MessageType status;
    public BigInteger hash;
    public BigInteger endHash; // non-inclusive
    public HashMap<String, BigInteger> order;
    public String data;
    public ECSNode receiverNode;

    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    public enum MessageType {
        SET_METADATA, 
        MOVE_DATA,

        START, // start KVServer,
        STOP,  // stop KVServer
        SHUTDOWN,
        LOCKED,
        UNLOCK, // unlock
        CLEAR_STORAGE, // clears persistant storage
        DELETE_STORAGE, // deletes persistant storage

        
        // TBD,                // placeholder status
        // ACK,                // Acknowledgment
        // DONE,                // Done data transfer
        // UPDATE,             // needs to update metadata (new server added next to it)
		// DATA,               // data transfer
        // STOP,               // Stop KVServer
        // SHUTDOWN,           // KVServer is stopped, respond to neither ECS nor KVClient 
        // DIE,           // KVServer is decommissioned
    }

    public Metadata(MessageType status, HashMap<String, BigInteger> order, ECSNode receiverNode) {
        this.status = status;
        this.order = order;
        this.receiverNode = receiverNode;
    }

    public MessageType getStatus() {
        return this.status;
    }

    public HashMap<String, BigInteger> getOrder(){
        return this.order;
    }

    public void setStatus(MessageType status) {
        this.status = status;
    }

    public ECSNode getReceiverNode() {
        return this.receiverNode;
    }

    @Override
    public String toString(){
        Gson gsonObj = new Gson();
        return gsonObj.toJson(this);
        // return "Server: " + this.name + ":" + this.port
        //         + "\n\tbeginning hash: " + this.hash
        //         + "\n\tend hash: " + this.endHash;
    }

    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    private byte[] toByteArray(String s) {
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[] { LINE_FEED, RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public byte[] getBytes() {
        String msg = this.toString();
        return addCtrChars(toByteArray(msg));
    }

    public String byteToString(byte[] msgBytes){
        // turn bytes to string
        byte[] tmp = addCtrChars(msgBytes);
        String jsonStr = new String(tmp);

        return jsonStr;
    }

}
