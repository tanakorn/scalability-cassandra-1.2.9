package edu.uchicago.cs.ucare.scale.gossip;

public class Gossip {
    
    public final int version;
    
    public Gossip(int version) {
        this.version = version;
    }
    
    public String toString() {
        return "v" + version;
    }

}
