package ecs;

import org.apache.log4j.Logger;


public class ECSNode implements IECSNode{

    private static Logger logger = Logger.getRootLogger();
    
    private String name;
    private String host;
    private int port;
    private String[] nodeHashRange;

    enum NodeStatus{
        WRITELOCK, // added but being written to
        OFFLINE, // not added
        READY, // ready
        STARTING, // added, waiting to be written to
        STOPPED, // removed but not yet offline
    }
    
    public NodeStatus status;

    public ECSNode(String name, int port, String host){
        // initializing a node
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public void setStatus(NodeStatus inStatus){
        this.status = inStatus;
    }

    public NodeStatus getStatus(){
        return status;
    }

    public void setStatus(String inStatus){
        NodeStatus enumStatus = NodeStatus.valueOf(inStatus);
        this.status = enumStatus;
    }

    public String toString(){
        return "ECSNode:\n"
                +"name: " + name
                +"\nport: " + port
                +"\nstatus: " + status
                +"\nhashrange: " + nodeHashRange;
    }

    @Override
    public String[] getNodeHashRange() {
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

}