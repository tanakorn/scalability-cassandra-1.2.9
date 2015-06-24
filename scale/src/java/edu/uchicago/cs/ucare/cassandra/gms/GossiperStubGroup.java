package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import edu.uchicago.cs.ucare.scale.InetAddressStubGroup;

public class GossiperStubGroup implements InetAddressStubGroup<GossiperStub> {
    
    Map<InetAddress, GossiperStub> stubs;
    String clusterId;
    int numNodes;
    int numTokens;
    @SuppressWarnings("rawtypes") IPartitioner partitioner;
    OmniscientGossiperStub omniStub;

    GossiperStubGroup(String clusterId, String dataCenter, int numNodes, int numTokens,
            @SuppressWarnings("rawtypes") IPartitioner partitioner)
            throws UnknownHostException {
        this.clusterId = clusterId;
        this.numNodes = numNodes;
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubs = new HashMap<InetAddress, GossiperStub>();
        for (int i = 0; i < numNodes; ++i) {
            InetAddress address = InetAddress.getByName("127.0.0." + (i + 2));
            stubs.put(address, new GossiperStub(address, clusterId, dataCenter, 
                    numTokens, partitioner));
        }
        omniStub = new OmniscientGossiperStub();
    }
    
    GossiperStubGroup(String clusterId, String dataCenter, Collection<GossiperStub> stubList, 
            int numTokens, @SuppressWarnings("rawtypes") IPartitioner partitioner) {
        this.clusterId = clusterId;
        this.numNodes = stubList.size();
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubs = new HashMap<InetAddress, GossiperStub>();
        for (GossiperStub stub : stubList) {
            stubs.put(stub.getInetAddress(), stub);
        }
        omniStub = new OmniscientGossiperStub();
    }

    void prepareInitialState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.prepareInitialState();
        }
    }

    void setupTokenState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setupTokenState();
        }
    }

    void setBootStrappingStatusState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setBootStrappingStatusState();
        }
    }

    public void setNormalStatusState() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setNormalStatusState();
        }
    }

    void setSeverityState(double severity) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setSeverityState(severity);
        }
    }

    void setLoad(double load) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.setLoad(load);
        }
    }

    void sendGossip(InetAddress node) {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.sendGossip(node);
        }
    }
    
    void sendOmniscientGossip(InetAddress node, InetAddress onBehalfOf) {
        GossiperStub stub = stubs.get(onBehalfOf);
        MessageOut<GossipDigestSyn> gossipSync = omniStub.genGossipDigestSyncMsg(onBehalfOf);
        stub.sendMessage(node, gossipSync);
    }

    void updateHeartBeat() {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.updateHeartBeat();
        }
    }

    void listen() throws ConfigurationException {
        for (InetAddress address : stubs.keySet()) {
            GossiperStub stub = stubs.get(address);
            stub.listen();
        }
    }

    @Override
    public Collection<GossiperStub> getAllStubs() {
        return stubs.values();
    }

    @Override
    public Collection<InetAddress> getAllInetAddress() {
        return stubs.keySet();
    }
    
    @Override
    public GossiperStub getStub(InetAddress address) {
        return stubs.get(address);
    }

    @Override
    public Iterator<GossiperStub> iterator() {
        return stubs.values().iterator();
    }
    
    public OmniscientGossiperStub getOmniscientGossiperStub() {
        return omniStub;
    }
    
    class OmniscientGossiperStub {
        
        Map<InetAddress, Map<Integer, ClockEndpointState>> testNodeStateByVersion;
        
        public OmniscientGossiperStub() {
            testNodeStateByVersion = new HashMap<InetAddress, Map<Integer, ClockEndpointState>>();
        }
        
        public MessageOut<GossipDigestSyn> genGossipDigestSyncMsg(InetAddress onBehalfOf) {
            Random random = new Random();
            List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
            EndpointState epState;
            int generation = 0;
            int maxVersion = 0;
            List<InetAddress> endpoints = new ArrayList<InetAddress>(stubs.keySet());
            Collections.shuffle(endpoints, random);
            for (InetAddress endpoint : endpoints) {
                epState = stubs.get(endpoint).getEndpointState();
                if (epState != null) {
                    generation = epState.getHeartBeatState().getGeneration();
                    maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
                }
                gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
            }
            GossipDigestSyn digestSynMessage = new GossipDigestSyn(clusterId, partitioner.getClass().getName(),
                    gossipDigestList);
            MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(onBehalfOf, 
                    MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
            return message;
        }
        
        class ClockEndpointState {
            
            int logicalClock;
            EndpointState endpointState;
            
            public ClockEndpointState(EndpointState endpointState) {
                logicalClock = 0;
                this.endpointState = endpointState;
            }
            
            public ClockEndpointState(int logicalClock, EndpointState endpointState) {
                this.logicalClock = logicalClock;
                this.endpointState = endpointState;
            }
            
            public int increaseClock() {
                return ++logicalClock;
            }
            
            public int getClock() {
                return logicalClock;
            }
            
        }
        
    }

}
