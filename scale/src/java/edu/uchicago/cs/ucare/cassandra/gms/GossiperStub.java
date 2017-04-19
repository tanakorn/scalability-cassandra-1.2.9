package edu.uchicago.cs.ucare.cassandra.gms;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uchicago.cs.ucare.scale.InetAddressStub;

public class GossiperStub implements InetAddressStub, IFailureDetectionEventListener {
	
	private static final UUID EMPTY_SCHEMA;
    static {
        try {
            EMPTY_SCHEMA = UUID.nameUUIDFromBytes(MessageDigest.getInstance("MD5").digest());
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError();
        }
    }
    static final List<String> DEAD_STATES = Arrays.asList(VersionedValue.REMOVING_TOKEN, VersionedValue.REMOVED_TOKEN,
            VersionedValue.STATUS_LEFT, VersionedValue.HIBERNATE);
    
    private static final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1,  InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };
    private static final Logger logger = LoggerFactory.getLogger(GossiperStub.class);
    
    public final Map<String, Long> syncReceivedTime = new ConcurrentHashMap<String, Long>();
    public final Map<String, Integer> ackNewVersionNormal = new ConcurrentHashMap<String, Integer>();
    public final Map<String, Integer> ackNewVersionBoot = new ConcurrentHashMap<String, Integer>();
    
    public int flapping;

    String clusterId;
	String dataCenter;
	InetAddress broadcastAddress;
	UUID hostId;
	UUID schema;
	public HeartBeatState heartBeatState;
	int numTokens;
	EndpointState state;
	ConcurrentMap<InetAddress, EndpointState> endpointStateMap;
	VersionedValueFactory versionedValueFactory;
	TokenMetadata tokenMetadata;
	@SuppressWarnings("rawtypes") public Collection<Token> tokens;
	@SuppressWarnings("rawtypes") IPartitioner partitioner;
	String partitionerName;
	public FailureDetector failureDetector;
	Set<InetAddress> liveEndpoints;
    Map<InetAddress, Long> unreachableEndpoints;
    Map<InetAddress, Long> justRemovedEndpoints;
    Map<InetAddress, Long> expireTimeEndpointMap;
    Set<InetAddress> seeds;
	
    
	boolean hasContactedSeed;
	
	private static final Random random = new Random();
	public static InetAddress getRandomAddress(Collection<InetAddress> addressCollection) {
	    if (addressCollection.isEmpty()) {
	        return null;
	    }
	    int size = addressCollection.size();
	    InetAddress[] addresses = new InetAddress[size];
	    addresses = addressCollection.toArray(addresses);
	    int randNum = random.nextInt(size);
	    return addresses[randNum];
	}
	
	GossiperStub(InetAddress broadcastAddress, String clusterId, String dataCenter, int numTokens,
			Set<InetAddress> seeds, @SuppressWarnings("rawtypes") IPartitioner partitioner) {
		this(broadcastAddress, clusterId, dataCenter, UUID.randomUUID(), EMPTY_SCHEMA, 
				new HeartBeatState((int) System.currentTimeMillis()), 
				numTokens, seeds, partitioner);
	}
	
	GossiperStub(InetAddress broadcastAddress, String clusterId, String dataCenter, 
			UUID hostId, UUID schema, HeartBeatState heartBeatState, int numTokens,
			Set<InetAddress> seeds, @SuppressWarnings("rawtypes") IPartitioner partitioner) {
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
		tokenMetadata = new TokenMetadata();
		failureDetector = new FailureDetector();
		failureDetector.setAddress(broadcastAddress);
		failureDetector.registerFailureDetectionEventListener(this);
        liveEndpoints = new ConcurrentSkipListSet<InetAddress>(inetcomparator);
        unreachableEndpoints = new ConcurrentHashMap<InetAddress, Long>();
        justRemovedEndpoints = new ConcurrentHashMap<InetAddress, Long>();
        expireTimeEndpointMap = new ConcurrentHashMap<InetAddress, Long>();
        this.seeds = new ConcurrentSkipListSet<InetAddress>(inetcomparator);
        if (seeds != null) {
            this.seeds.addAll(seeds);
        }
        flapping = 0;
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
		tokens = BootStrapper.getRandomTokens(tokenMetadata, numTokens);
	}
	
	public void setupTokenState() {
		if (tokens == null) {
			initTokens();
		}
        state.addApplicationState(ApplicationState.TOKENS, versionedValueFactory.tokens(tokens));
	}
	
	public void updateNormalTokens() {
        tokenMetadata.updateNormalTokens(tokens, broadcastAddress);
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
	
	public TokenMetadata getTokenMetadata() {
	    return tokenMetadata;
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
	
   public MessageOut<GossipDigestSyn> genGossipDigestSyncMsgOut(InetAddress to) {
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
        MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddress, to,
                MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
        return message;
    }
   
   private static Map<String, byte[]> emptyMap = Collections.<String, byte[]>emptyMap();
   public MessageIn<GossipDigestSyn> genGossipDigestSyncMsgIn(InetAddress to) {
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
       MessageIn<GossipDigestSyn> message = MessageIn.create(broadcastAddress, digestSynMessage, 
               emptyMap, MessagingService.Verb.GOSSIP_DIGEST_SYN, MessagingService.VERSION_12);
       message.setTo(to);
       message.createdTime = System.currentTimeMillis();
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

    public FailureDetector getFailureDetector() {
        return failureDetector;
    }

    public void setFailureDetector(FailureDetector failureDetector) {
        this.failureDetector = failureDetector;
    }
    
    public Set<InetAddress> getLiveEndpoints() {
        return liveEndpoints;
    }

    public void setLiveEndpoints(Set<InetAddress> liveEndpoints) {
        this.liveEndpoints = liveEndpoints;
    }

    public Map<InetAddress, Long> getUnreachableEndpoints() {
        return unreachableEndpoints;
    }

    public void setUnreachableEndpoints(Map<InetAddress, Long> unreachableEndpoints) {
        this.unreachableEndpoints = unreachableEndpoints;
    }

    public Set<InetAddress> getSeeds() {
        return seeds;
    }

    public void setSeeds(Set<InetAddress> seeds) {
        this.seeds = seeds;
    }
    
    public Map<InetAddress, Long> getJustRemovedEndpoints() {
        return justRemovedEndpoints;
    }

    public void setJustRemovedEndpoints(Map<InetAddress, Long> justRemovedEndpoints) {
        this.justRemovedEndpoints = justRemovedEndpoints;
    }

    public void doStatusCheck() {
        /*long now = TimePreservingService.getCurrentTimeMillis(WholeClusterSimulator.isReplayEnabled);

        Set<InetAddress> eps = endpointStateMap.keySet();
//        StringBuilder sb = new StringBuilder(broadcastAddress + " allphi : ");
        for ( InetAddress endpoint : eps ) {
            if (endpoint.equals(broadcastAddress)) {
                continue;
            }

            double phi = failureDetector.interpret(endpoint);
//            sb.append(phi);
//            sb.append(',');
            EndpointState epState = endpointStateMap.get(endpoint);
            if ( epState != null ) {
                // check for dead state removal
                long expireTime = getExpireTimeForEndpoint(endpoint);
                if (!epState.isAlive() && (now > expireTime)
                        && (!StorageService.instance.getTokenMetadata().isMember(endpoint))) {
                    evictFromMembership(endpoint);
                }
            }
        }
//        logger.info(sb.toString());

        if (!justRemovedEndpoints.isEmpty()) {
            for (Entry<InetAddress, Long> entry : justRemovedEndpoints.entrySet()) {
                if ((now - entry.getValue()) > WholeClusterSimulator.QUARANTINE_DELAY) {
                    justRemovedEndpoints.remove(entry.getKey());
                }
            }
        }*/
    }

    public static long computeExpireTime() {
        return TimePreservingService.getCurrentTimeMillis(WholeClusterSimulator.isReplayEnabled) + Gossiper.aVeryLongTime;
    }

    protected long getExpireTimeForEndpoint(InetAddress endpoint) {
        /* default expireTime is aVeryLongTime */
        Long storedTime = expireTimeEndpointMap.get(endpoint);
        return storedTime == null ? computeExpireTime() : storedTime;
    }
    
    private void evictFromMembership(InetAddress endpoint) {
        unreachableEndpoints.remove(endpoint);
        endpointStateMap.remove(endpoint);
        expireTimeEndpointMap.remove(endpoint);
        quarantineEndpoint(endpoint);
    }
    
    private void quarantineEndpoint(InetAddress endpoint) {
        justRemovedEndpoints.put(endpoint, System.currentTimeMillis());
    }
    
    private Boolean isDeadState(EndpointState epState)
    {
        if (epState.getApplicationState(ApplicationState.STATUS) == null)
            return false;
        String value = epState.getApplicationState(ApplicationState.STATUS).value;
        String[] pieces = value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        String state = pieces[0];
        for (String deadstate : DEAD_STATES) {
            if (state.equals(deadstate))
                return true;
        }
        return false;
    }
    
    private void markDead(InetAddress addr, EndpointState localState, double phi) {
//        logger.info(broadcastAddress + " convict " + addr + " with phi " + phi);
        /*flapping++;
        localState.markDead();
        liveEndpoints.remove(addr);
        unreachableEndpoints.put(addr, TimePreservingService.getCurrentTimeMillis(WholeClusterSimulator.isReplayEnabled));*/
    }

    @Override
    public void convict(InetAddress ep, double phi) {
        // TODO Auto-generated method stub
        EndpointState epState = endpointStateMap.get(ep);
        if (epState.isAlive() && !isDeadState(epState)) {
            markDead(ep, epState, phi);
        }
        else {
            epState.markDead();
        }
    }
    
    public void markAlive(InetAddress addr, EndpointState localState) {
        /*localState.markAlive();
        localState.updateTimestamp(); // prevents doStatusCheck from racing us and evicting if it was down > aVeryLongTime
        liveEndpoints.add(addr);
        unreachableEndpoints.remove(addr);
        expireTimeEndpointMap.remove(addr);*/
    }
    
    @Override
    public String toString() {
        return broadcastAddress.toString();
    }
    
    public void removeEndpoint(InetAddress endpoint)
    {
        if(seeds.contains(endpoint))
        {
            seeds.remove(endpoint);
        }

        liveEndpoints.remove(endpoint);
        unreachableEndpoints.remove(endpoint);
        // do not remove endpointState until the quarantine expires
        failureDetector.remove(endpoint);
        quarantineEndpoint(endpoint);
    }

}
