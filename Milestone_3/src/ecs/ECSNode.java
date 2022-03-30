package ecs;

import java.math.BigInteger;

import org.apache.log4j.Logger;

import app_kvECS.ECSConnection;
import shared.messages.Metadata;
import shared.messages.JSONMessage;

public class ECSNode implements IECSNode {

    private static Logger logger = Logger.getRootLogger();

    private String name;
    private String host;
    private int port;
    private int replicateReceiverPort;
    private String[] nodeHashRange;
    private ECSConnection ecsConnection;

    private BigInteger hash;
    private BigInteger endHash;

    public NodeStatus status;

    private String cacheAlgo;
    private int cacheSize;

    public ECSNode(String name, int port, String host) {
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public ECSNode(String name, String port, String host) {
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = Integer.parseInt(port);
    }

    public ECSNode(String name, int port, String host, NodeStatus inStatus) {
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
        this.status = inStatus;
    }

    public ECSNode(String name, int port, String host, NodeStatus inStatus, int replicateReceiverPort) {
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
        this.status = inStatus;
        this.replicateReceiverPort = replicateReceiverPort;
    }

    public ECSNode(String host, int port, BigInteger inHash) {
        // initializing a node just to be used for temporary node in KVStore
        this.host = host;
        this.port = port;
        this.hash = inHash;
    }

    public ECSNode(String name, String host, int port, BigInteger inHash, BigInteger endHash) {
        // initializing a node just to be used for temporary node in KVStore
        this.name = name;
        this.host = host;
        this.port = port;
        this.hash = inHash;
        this.endHash = endHash;
    }

    public void setStatus(NodeStatus inStatus) {
        this.status = inStatus;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setConnection(ECSConnection ecsConnection) {
        this.ecsConnection = ecsConnection;
    }

    public void sendMessage(Metadata msg) {
        try {
            this.ecsConnection.sendJSONMessage(msg);
        } catch (Exception e) {
            logger.info("Unable to send message: " + e);
        }
    }

    public JSONMessage receiveMessage() {
        try {
            return this.ecsConnection.receiveJSONMessage();
        } catch (Exception e) {
            logger.info("Unable to receive message: " + e);
        }
        return null;
    }

    public void setStatus(String inStatus) {
        NodeStatus enumStatus = NodeStatus.valueOf(inStatus);
        this.status = enumStatus;
    }

    @Override
    public String toString() {
        return "ECSNode:\n"
                + "name: " + name
                + "\nport: " + port
                + "\nstatus: " + status
                + "\nhashrange: " + nodeHashRange;
    }

    @Override
    public String[] getNodeHashRange() {
        // TODO get hash range
        return null;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    public String getNamePortHost() {
        return name + ":" + port + ":" + host;
    }

    public BigInteger getHash() {
        return hash;
    }

    public BigInteger getEndHash() {
        return endHash;
    }

    public int getReplicateReceiverPort() {
        return this.replicateReceiverPort;
    }

    public void setReplicateReceiverPort(int replicatePort) {
        this.replicateReceiverPort = replicatePort;
    }

    public void setHashRange(BigInteger hashVal, BigInteger endVal) {
        this.hash = hashVal;
        this.endHash = endVal;
    }

    public void setHash(BigInteger hashVal) {
        this.hash = hashVal;
    }

    public void setCacheInfo(int cacheSize, String cacheAlgo) {
        this.cacheSize = cacheSize;
        this.cacheAlgo = cacheAlgo;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public String getCacheStrategy() {
        return this.cacheAlgo;
    }

}