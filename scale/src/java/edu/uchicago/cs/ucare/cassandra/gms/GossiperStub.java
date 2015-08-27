package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import edu.uchicago.cs.ucare.scale.InetAddressStub;

public class GossiperStub implements InetAddressStub {
	
	private static final UUID EMPTY_SCHEMA;
    static {
        try {
            EMPTY_SCHEMA = UUID.nameUUIDFromBytes(MessageDigest.getInstance("MD5").digest());
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError();
        }
    }
	
    String clusterId;
	String dataCenter;
	InetAddress broadcastAddress;
	UUID hostId;
	UUID schema;
	HeartBeatState heartBeatState;
	int numTokens;
	EndpointState state;
	ConcurrentMap<InetAddress, EndpointState> endpointStateMap;
	VersionedValueFactory versionedValueFactory;
	@SuppressWarnings("rawtypes") Collection<Token> tokens;
	@SuppressWarnings("rawtypes") IPartitioner partitioner;
	String partitionerName;
	
	boolean hasContactedSeed;
	
	GossiperStub(InetAddress broadcastAddress, String clusterId, String dataCenter, int numTokens,
			@SuppressWarnings("rawtypes") IPartitioner partitioner) {
		this(broadcastAddress, clusterId, dataCenter, UUID.randomUUID(), EMPTY_SCHEMA, 
				new HeartBeatState((int) System.currentTimeMillis()), numTokens, partitioner);
	}
	
	GossiperStub(InetAddress broadcastAddress, String clusterId, String dataCenter, 
			UUID hostId, UUID schema, HeartBeatState heartBeatState, int numTokens,
			@SuppressWarnings("rawtypes") IPartitioner partitioner) {
		this.clusterId = clusterId;
		this.dataCenter = dataCenter;
		this.broadcastAddress = broadcastAddress;
		this.hostId = hostId;
		this.schema = schema;
		this.heartBeatState = heartBeatState;
		this.numTokens = numTokens;
		this.partitioner = partitioner;
		partitionerName = partitioner.getClass().getName();
		endpointStateMap = new ConcurrentHashMap<InetAddress, EndpointState>();
		state = new EndpointState(heartBeatState);
		versionedValueFactory = new VersionedValueFactory(partitioner);
		hasContactedSeed = false;
	}
	
	public void prepareInitialState() {
		state.addApplicationState(ApplicationState.DC, versionedValueFactory.datacenter(dataCenter));
		state.addApplicationState(ApplicationState.HOST_ID, versionedValueFactory.hostId(hostId));
		state.addApplicationState(ApplicationState.NET_VERSION, versionedValueFactory.networkVersion());
		state.addApplicationState(ApplicationState.RPC_ADDRESS, versionedValueFactory.rpcaddress(broadcastAddress));
		state.addApplicationState(ApplicationState.SCHEMA, versionedValueFactory.schema(schema));
		endpointStateMap.put(broadcastAddress, state);
	}
	
	private void initTokens() {
		TokenMetadata tokenMetadata = new TokenMetadata();
		tokens = BootStrapper.getRandomTokens(tokenMetadata, numTokens);
	}
	
	public void setupTokenState() {
		if (tokens == null) {
			initTokens();
		}
        state.addApplicationState(ApplicationState.TOKENS, versionedValueFactory.tokens(tokens));
	}
	
	public void setBootStrappingStatusState() {
		state.addApplicationState(ApplicationState.STATUS, versionedValueFactory.bootstrapping(tokens));
	}

	public void setNormalStatusState() {
		state.addApplicationState(ApplicationState.STATUS, versionedValueFactory.normal(tokens));
	}
	
	public void setSeverityState(double severity) {
		state.addApplicationState(ApplicationState.SEVERITY, versionedValueFactory.severity(severity));
	}
	
	public void setLoad(double load) {
		state.addApplicationState(ApplicationState.LOAD, versionedValueFactory.load(load));
	}
	
	public EndpointState getEndpointState() {
	    return endpointStateMap.get(broadcastAddress);
	}

	public ConcurrentMap<InetAddress, EndpointState> getEndpointStateMap() {
		return endpointStateMap;
	}
	
	public void updateHeartBeat() {
		heartBeatState.updateHeartBeat();
	}
	
	public void listen() throws ConfigurationException {
        MessagingService.instance().listen(broadcastAddress);
	}
	
	public MessageOut<GossipDigestSyn> genGossipDigestSyncMsg() {
		Random random = new Random();
		List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
        EndpointState epState;
        int generation = 0;
        int maxVersion = 0;
        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
        for (InetAddress endpoint : endpoints) {
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
	
	public GossipDigest createGossipDigest() {
	    EndpointState endpointState = endpointStateMap.get(broadcastAddress);
	    int generation = endpointState.getHeartBeatState().getGeneration();
	    int maxVersion = endpointState.getHeartBeatState().getHeartBeatVersion();
	    return new GossipDigest(broadcastAddress, generation, maxVersion);
	}
	
	public void sendGossip(InetAddress to) {
		MessageOut<GossipDigestSyn> gds = genGossipDigestSyncMsg();
		MessagingService.instance().sendOneWay(gds, to);
	}
	
	public boolean setEndpointStateIfNewer(InetAddress address, EndpointState epState) {
	    if (endpointStateMap.containsKey(address)) {
	        EndpointState oldState = endpointStateMap.get(address);
	        if (oldState.getHeartBeatState().getHeartBeatVersion() >= epState.getHeartBeatState().getHeartBeatVersion()) {
                endpointStateMap.put(address, epState);
                return true;
	        }
	        return false;
	    }
	    endpointStateMap.put(address, epState);
	    return true;
	}
	
	public void sendMessage(InetAddress to, MessageOut<?> message) {
	    MessagingService.instance().sendOneWay(message, to);
	}

    @Override
    public InetAddress getInetAddress() {
        return broadcastAddress;
    }

    public boolean getHasContactedSeed() {
        return hasContactedSeed;
    }

    public void setHasContactedSeed(boolean hasContactedSeed) {
        this.hasContactedSeed = hasContactedSeed;
    }

}
