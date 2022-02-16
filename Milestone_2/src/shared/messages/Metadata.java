package shared.messages;

import java.io.Serializable;
import java.math.BigInteger;

public class Metadata implements Serializable {
    
    public String name;
    public BigInteger hash;
    public BigInteger endHash; // non-inclusive
    public int port;

    public Metadata(String inName, BigInteger inHash, BigInteger inEndHash, int inPort) {
        this.name = inName;
        this.hash = inHash;
        this.endHash = inEndHash;
        this.port = inPort;
    }

    @Override
    public String toString(){
        return "Server: " + this.name + ":" + this.port
                + "\n\tbeginning hash: " + this.hash
                + "\n\tend hash: " + this.endHash;
    }

}
