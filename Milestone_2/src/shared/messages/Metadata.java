package shared.messages;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;

import com.google.gson.Gson;

public class Metadata implements Serializable {
    
    public String name;
    public BigInteger hash;
    public BigInteger endHash; // non-inclusive
    public int port;
    public MessageType status;
    public HashMap<String, BigInteger> order;
    public String data;


    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    public enum MessageType {
        TBD,                // placeholder status
        ACK,                // Acknowledgment
        DONE,                // Done data transfer
        UPDATE,             // needs to update metadata (new server added next to it)
		DATA,               // data transfer
        STOP,               // Stop KVServer
        SHUTDOWN,           // KVServer is stopped, respond to neither ECS nor KVClient 
    }

    public Metadata() {
        this.status = MessageType.TBD;
    }

    public Metadata(String inName, BigInteger inHash, BigInteger inEndHash, int inPort) {
        this.name = inName;
        this.hash = inHash;
        this.endHash = inEndHash;
        this.port = inPort;
        this.status = MessageType.TBD;
    }

    public Metadata(String inName, BigInteger inHash, BigInteger inEndHash, int inPort, MessageType status, HashMap<String, BigInteger> order) {
        this.name = inName;
        this.hash = inHash;
        this.endHash = inEndHash;
        this.port = inPort;
        this.status = status;
        this.order = order;
    }

    public Metadata(String data) {
        this.status = MessageType.DATA;
        this.data = data;
    }

    public void setOrder(HashMap<String, BigInteger> order){
        this.order = order;
    }

    public void setStatus(MessageType status){
        this.status = status;
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

}
