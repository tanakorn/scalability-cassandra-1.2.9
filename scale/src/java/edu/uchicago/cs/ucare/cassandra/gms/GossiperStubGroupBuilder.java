package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.dht.IPartitioner;

public class GossiperStubGroupBuilder {
    
    String clusterId;
    String dataCenter;
    Collection<InetAddress> addressList;
    int numTokens;
    @SuppressWarnings("rawtypes") IPartitioner partitioner;

    public String getClusterId() {
        return clusterId;
    }
    
    public GossiperStubGroupBuilder setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }
    
    public String getDataCenter() {
        return dataCenter;
    }

    public GossiperStubGroupBuilder setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
        return this;
    }

    public Collection<InetAddress> getAddressList() {
        return addressList;
    }
    
    public GossiperStubGroupBuilder setAddressList(Collection<InetAddress> addressList) {
        this.addressList = addressList;
        return this;
    }
    
    public int getNumTokens() {
        return numTokens;
    }
    
    public GossiperStubGroupBuilder setNumTokens(int numTokens) {
        this.numTokens = numTokens;
        return this;
    }
    
    @SuppressWarnings("rawtypes")
    public IPartitioner getPartitioner() {
        return partitioner;
    }
    
    @SuppressWarnings("rawtypes")
    public GossiperStubGroupBuilder setPartitioner(IPartitioner partitioner) {
        this.partitioner = partitioner;
        return this;
    }
    
    public GossiperStubGroup build() {
        List<GossiperStub> stubs = new LinkedList<GossiperStub>();
        if (addressList != null) {
            for (InetAddress address : addressList) {
                stubs.add(createGossiperStub(address));
            }
        }
        return new GossiperStubGroup(clusterId, dataCenter, stubs, numTokens, partitioner);
    }
    
    GossiperStub createGossiperStub(InetAddress address) {
        return new GossiperStub(address, clusterId, dataCenter, numTokens, partitioner);
    }
    
}
