package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;

import edu.uchicago.cs.ucare.scale.InetAddressStubGroup;

public class GossiperStubGroup implements InetAddressStubGroup<GossiperStub> {
    
    Map<InetAddress, GossiperStub> stubs;
    String clusterId;
    int numNodes;
    int numTokens;
    @SuppressWarnings("rawtypes") IPartitioner partitioner;

    GossiperStubGroup(String clusterId, String dataCenter, int numNodes, int numTokens,
            Set<InetAddress> seeds, @SuppressWarnings("rawtypes") IPartitioner partitioner)
            throws UnknownHostException {
        this.clusterId = clusterId;
        this.numNodes = numNodes;
        this.numTokens = numTokens;
        this.partitioner = partitioner;
        stubs = new HashMap<InetAddress, GossiperStub>();
        for (int i = 0; i < numNodes; ++i) {
            InetAddress address = InetAddress.getByName("127.0.0." + (i + 2));
            stubs.put(address, new GossiperStub(address, clusterId, dataCenter, 
                    numTokens, seeds, partitioner));
        }
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
    
    public GossiperStub getRandomStub() {
        Random rand = new Random();
        GossiperStub[] stubArray = new GossiperStub[numNodes];
        stubArray = stubs.values().toArray(stubArray);
        return stubArray[rand.nextInt(numNodes)];
    }
    
    public boolean contains(InetAddress address) {
        return stubs.containsKey(address);
    }

}
