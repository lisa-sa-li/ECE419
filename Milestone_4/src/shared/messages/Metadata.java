package shared.messages;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;

import ecs.ECSNode;

import com.google.gson.Gson;

// public class Metadata implements Serializable {
public class Metadata {

    public MessageType status;
    // public BigInteger hash;
    // public BigInteger endHash; // non-inclusive
    public HashMap<String, BigInteger> order;
    // public String data;
    // public ECSNode receiverNode;
    public int receiverNodePort;
    public String receiverNodeName;
    public String receiverNodeHost;
    public BigInteger receiveNodeHash;
    public BigInteger receiveNodeEndHash;

    public String receiverNodeStr;

    public enum MessageType {
        SET_METADATA,
        MOVE_DATA,

        START, // start KVServer,
        STOP, // stop KVServer
        SHUTDOWN,
        LOCKED,
        UNLOCK, // unlock
        CLEAR_STORAGE, // clears persistant storage
        DELETE_STORAGE, // deletes persistant storage

        SERVER_NOT_RESPONSIBLE,
    }

    public Metadata(MessageType status, HashMap<String, BigInteger> order, ECSNode receiverNode) {
        this.status = status;
        this.order = order;
        if (receiverNode != null) {
            this.receiverNodeName = receiverNode.getNodeName();
            this.receiverNodePort = receiverNode.getNodePort();
            this.receiverNodeHost = receiverNode.getNodeHost();
            this.receiveNodeHash = receiverNode.getHash();
            this.receiveNodeEndHash = receiverNode.getEndHash();
        }
    }

    public MessageType getStatus() {
        return this.status;
    }

    public HashMap<String, BigInteger> getOrder() {
        return this.order;
    }

    public void setStatus(MessageType status) {
        this.status = status;
    }

    public ECSNode getReceiverNode() {
        return new ECSNode(receiverNodeName, receiverNodeHost, receiverNodePort, receiveNodeHash, receiveNodeEndHash);
    }

    @Override
    public String toString() {
        // Gson gsonObj = new Gson();
        // return gsonObj.toJson(this);
        return "Status: " + this.status
                + "\n\tmapping: " + this.order;
    }

}
