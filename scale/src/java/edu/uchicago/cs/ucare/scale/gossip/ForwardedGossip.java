package edu.uchicago.cs.ucare.scale.gossip;

import java.util.LinkedList;
import java.util.List;

public class ForwardedGossip {
    
    public final Gossip gossip;
    private final LinkedList<ForwardEvent> forwardHistory;

    public ForwardedGossip(Gossip gossip, int initialPeer, int receivedTime) {
        this.gossip = gossip;
        forwardHistory = new LinkedList<ForwardedGossip.ForwardEvent>();
        forwardHistory.add(new ForwardEvent(initialPeer, receivedTime));
    }
    
    private ForwardedGossip(Gossip gossip, List<ForwardEvent> forwardHistory) {
        this.gossip = gossip;
        this.forwardHistory = new LinkedList<ForwardedGossip.ForwardEvent>(forwardHistory);
    }
    
    public ForwardedGossip forward(int sender, int receivedTime) {
        ForwardedGossip nextGossip = new ForwardedGossip(gossip, forwardHistory);
        nextGossip.forwardHistory.add(new ForwardEvent(sender, receivedTime));
        return nextGossip;
    }
    
    public String toString() {
        String s = "{" + gossip.toString() + ":";
        for (ForwardEvent event : forwardHistory) {
            s += event.toString() + ",";
        }
        s += "}";
        return s;
    }
    
    public LinkedList<ForwardEvent> forwardHistory() {
        return forwardHistory;
    }
    
    public static class ForwardEvent {

        public final int receiver;
        public final int receivedTime;

        public ForwardEvent(int receiver, int receivedTime) {
            this.receiver = receiver;
            this.receivedTime = receivedTime;
        }
        
        public String toString() {
            return "(peer=" + receiver + ",time=" + receivedTime + ")";
        }

    }

}
