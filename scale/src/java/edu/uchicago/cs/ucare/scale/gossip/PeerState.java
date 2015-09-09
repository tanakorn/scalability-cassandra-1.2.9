package edu.uchicago.cs.ucare.scale.gossip;

import java.util.LinkedList;

public class PeerState {
    
    private LinkedList<ForwardedGossip> gossipHistory;

    public PeerState() {
        gossipHistory = new LinkedList<ForwardedGossip>();
    }

    public void receive(ForwardedGossip receiveEvent) {
        gossipHistory.add(receiveEvent);
    }
    
    public boolean hasReceivedGossip() {
        return !gossipHistory.isEmpty();
    }
    
    public ForwardedGossip lastGossip() {
        return gossipHistory.peekLast();
    }
    
    public String toString() {
        String s = "[";
        for (ForwardedGossip receiveEvent : gossipHistory) {
            s += receiveEvent.toString();
            s += ";";
        }
        s += "]"; 
        return s;
    }
    
    @SuppressWarnings("unchecked")
    public PeerState copy() {
        PeerState copy = new PeerState();
        copy.gossipHistory = (LinkedList<ForwardedGossip>) this.gossipHistory.clone();
        return copy;
    }
    
    public LinkedList<ForwardedGossip> getModel() {
        return (LinkedList<ForwardedGossip>) gossipHistory.clone();
    }

}
