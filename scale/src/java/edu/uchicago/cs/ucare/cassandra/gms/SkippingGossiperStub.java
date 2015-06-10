package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class SkippingGossiperStub extends GossiperStub {
    
    Set<InetAddress> skipAddresses;

    SkippingGossiperStub(String clusterId, String dataCenter,
            InetAddress broadcastAddress, int numTokens,
            @SuppressWarnings("rawtypes") IPartitioner partitioner) {
        super(broadcastAddress, clusterId, dataCenter, numTokens, partitioner);
        skipAddresses = new HashSet<InetAddress>();
    }

    SkippingGossiperStub(String clusterId, String dataCenter,
            InetAddress broadcastAddress, int numTokens,
            @SuppressWarnings("rawtypes") IPartitioner partitioner,
            Collection<InetAddress> skipAddresses) {
        super(broadcastAddress, clusterId, dataCenter, numTokens, partitioner);
        this.skipAddresses = new HashSet<InetAddress>(skipAddresses);
    }
    
    public void addSkipAddress(InetAddress address) {
        skipAddresses.add(address);
    }
    
    public void addSkipAddresses(Collection<InetAddress> skipAddresses) {
        this.skipAddresses.addAll(skipAddresses);
    }
    
    @Override
    MessageOut<GossipDigestSyn> genGossipDigestSyncMsg() {
        Random random = new Random();
        List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
        EndpointState epState;
        int generation = 0;
        int maxVersion = 0;
        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
        for (InetAddress endpoint : endpoints) {
            if (skipAddresses.contains(endpoint)) {
                continue;
            }
            epState = endpointStateMap.get(endpoint);
            if (epState != null) {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
            }
            gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
        }
        GossipDigestSyn digestSynMessage = new GossipDigestSyn(clusterId, partitionerName, 
                gossipDigestList);
        MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddress, 
                MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
        return message;
    }

}
