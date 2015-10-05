package edu.uchicago.cs.ucare.scale.gossip;

import java.util.Random;

public class GossipPropagationSim {
    
    private static PeerState[] copy(PeerState[] peers) {
        PeerState[] copy = new PeerState[peers.length];
        for (int i = 0; i < peers.length; ++i) {
            copy[i] = peers[i].copy();
        }
        return copy;
    }
    
    public static PeerState[] simulate(int numPeer, int numRound) {
        Random rand = new Random();
        PeerState[] peers = new PeerState[numPeer];
        for (int i = 0; i < peers.length; ++i) {
            peers[i] = new PeerState();
        }
        for (int i = 0; i < numRound; ++i) {
            PeerState[] nextRoundPeers = copy(peers);
            for (int sendFrom = 0; sendFrom < peers.length; ++sendFrom) {
                if (peers[sendFrom].hasReceivedGossip()) {
                    int sendTo = sendFrom;
                    while (sendTo == sendFrom) {
                        sendTo = rand.nextInt(peers.length);
                    }
                    ForwardedGossip sendingGossip = peers[sendFrom].lastGossip();
                    if (!nextRoundPeers[sendTo].hasReceivedGossip() || 
                            nextRoundPeers[sendTo].lastGossip().gossip.version < sendingGossip.gossip.version) {
                        nextRoundPeers[sendTo].receive(sendingGossip.forward(sendTo, i));
                    }
                }
            }
            peers = nextRoundPeers;
            peers[0].receive(new ForwardedGossip(new Gossip(i), 0, i));
        }
        return peers;
    }

    public static void main(String[] args) {
        int numPeer = 128;
        int numRound = 30;
        PeerState[] peers = simulate(numPeer, numRound);
        for (PeerState peer : peers) {
            System.out.println(peer.toString());
        }
    }

}
