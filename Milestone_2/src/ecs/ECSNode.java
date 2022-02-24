package ecs;

import java.math.BigInteger;

import org.apache.log4j.Logger;

import app_kvECS.ECSConnection;
import shared.messages.Metadata;


public class ECSNode implements IECSNode {

    private static Logger logger = Logger.getRootLogger();
    
    private String name;
    private String host;
    private int id;
    private int port;
    private String[] nodeHashRange;
    private ECSConnection ecsConnection;

    private BigInteger hash;
    private BigInteger endHash;
    
    public NodeStatus status;

    public ECSNode(String name, int port, String host){
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public ECSNode(String name, int port, String host, NodeStatus inStatus){
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
        this.status = inStatus;
    }

    public ECSNode(String name, int port, String host, NodeStatus inStatus, int orderAdded){
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
        this.status = inStatus;
        this.id = orderAdded;
    }

    public void setStatus(NodeStatus inStatus){
        this.status = inStatus;
    }

    public NodeStatus getStatus(){
        return status;
    }

    public void setConnection(ECSConnection ecsConnection){
        this.ecsConnection = ecsConnection;
    }

    public void sendMessage(Metadata msg){
        try{
            this.ecsConnection.sendJSONMessage(msg);
        } catch (Exception e){
            logger.info("Unable to send message: " + e);
        }
    }

    public void setStatus(String inStatus){
        NodeStatus enumStatus = NodeStatus.valueOf(inStatus);
        this.status = enumStatus;
    }

    @Override
    public String toString(){
        return "ECSNode:\n"
                +"name: " + name
                +"\nport: " + port
                +"\nstatus: " + status
                +"\nhashrange: " + nodeHashRange;
    }

    @Override
    public String[] getNodeHashRange() {
        //TODO get hash range
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

    public BigInteger getHash() {
        return hash;
    }

    public BigInteger getEndHash() {
        return endHash;
    }

    public void setHashRange(BigInteger hashVal, BigInteger endVal){
        this.hash = hashVal;
        this.endHash = endVal;
    }

    public void setHash(BigInteger hashVal){
        this.hash = hashVal;
    }

}