package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

import edu.uchicago.cs.ucare.scale.InetAddressStubGroup;

public class GossiperStubGroup implements InetAddressStubGroup<GossiperStub> {
    
    Map<InetAddress, GossiperStub> stubsByAddress;
    GossiperStub[] stubsById;
    String clusterId;
    int numNodes;
    int numTokens;
    @SuppressWarnings("rawtypes") IPartitioner partitioner;

    GossiperStubGroup(String clusterId, String dataCenter, Collection<GossiperStub> stubList, 
            int numTokens, @SuppressWarnings("rawtypes") IPartitioner partitioner) {
        this.clusterId = clusterId;
        this.numNodes = stubList.size();
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubsByAddress = new HashMap<InetAddress, GossiperStub>();
        stubsById = new GossiperStub[numNodes];
        for (GossiperStub stub : stubList) {
            stubsByAddress.put(stub.getInetAddress(), stub);
            stubsById[stub.getId()] = stub;
        }
    }

    void prepareInitialState() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.prepareInitialState();
        }
    }

    void setupTokenState() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.setupTokenState();
        }
    }
    
    void updateNormalTokens() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.tokenMetadata.updateNormalTokens(stub.tokens, stub.broadcastAddress);
        }
    }

    void setBootStrappingStatusState() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.setBootStrappingStatusState();
        }
    }

    public void setNormalStatusState() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.setNormalStatusState();
        }
    }

    void setSeverityState(double severity) {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.setSeverityState(severity);
        }
    }

    void setLoad(double load) {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.setLoad(load);
        }
    }

    void sendGossip(InetAddress node) {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.sendGossip(node);
        }
    }
    
    void updateHeartBeat() {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.updateHeartBeat();
        }
    }

    void listen() throws ConfigurationException {
        for (InetAddress address : stubsByAddress.keySet()) {
            GossiperStub stub = stubsByAddress.get(address);
            stub.listen();
        }
    }

    @Override
    public Collection<GossiperStub> getAllStubs() {
        return stubsByAddress.values();
    }

    @Override
    public Collection<InetAddress> getAllInetAddress() {
        return stubsByAddress.keySet();
    }
    
    @Override
    public GossiperStub getStub(InetAddress address) {
        return stubsByAddress.get(address);
    }
    
    public GossiperStub getStub(int id) {
        return stubsById[id];
    }

    @Override
    public Iterator<GossiperStub> iterator() {
        return stubsByAddress.values().iterator();
    }
    
    public GossiperStub getRandomStub() {
        Random rand = new Random();
        GossiperStub[] stubArray = new GossiperStub[numNodes];
        stubArray = stubsByAddress.values().toArray(stubArray);
        return stubArray[rand.nextInt(numNodes)];
    }
    
    public boolean contains(InetAddress address) {
        return stubsByAddress.containsKey(address);
    }
    
    public int size() {
        return numNodes;
    }

}
