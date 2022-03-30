package ecs;

public interface IECSNode {

    public enum NodeStatus {
        OFFLINE, // not added
        STARTING, // added, waiting to be written to
        WRITELOCK, // added but being written to
        STOPPED, // removed but not yet offline
        SHUTTING_DOWN, // when we call shutdown or removenodes, but before it becomes OFFLINE
    }

    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return array of two strings representing the low and high range of the
     *         hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

}
