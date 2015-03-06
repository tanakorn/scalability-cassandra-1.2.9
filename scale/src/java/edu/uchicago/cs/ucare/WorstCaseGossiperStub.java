package edu.uchicago.cs.ucare;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorstCaseGossiperStub {
	
    private static final Logger logger = LoggerFactory.getLogger(WorstCaseGossiperStub.class);

    public static InetAddress[] broadcastAddresses;
    public static Set<InetAddress> addressSet;
	public static ConcurrentMap<InetAddress, ConcurrentMap<InetAddress, EndpointState>> endpointStateMapMap;
	public static ConcurrentMap<InetAddress, ConcurrentMap<MessageOut, InetAddress>> messageOutAddressMap;
	public static ConcurrentMap<MessageIn, InetAddress> messageInAddressMap;
	public static int currentNode;
    public static final AtomicInteger idGen = new AtomicInteger(0);
    public static InetAddress seed;
    public static InetAddress target;
    public static ConcurrentHashMap<InetAddress, CountDownLatch> countDowns;
    
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, ConfigurationException, InterruptedException {
		int allNodes = 13;
		String localDataCenter = "datacenter1";
		int numTokens = 1024;
		String addressPrefix = "127.0.0.";
		InetAddress localAddresses[] = new InetAddress[allNodes];
		broadcastAddresses = new InetAddress[allNodes];
		InetAddress rpcAddresses[] = new InetAddress[allNodes];
		HeartBeatState[] heartBeats = new HeartBeatState[allNodes];
		EndpointState[] states = new EndpointState[allNodes];
		IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
		VersionedValueFactory[] valueFactories = new VersionedValueFactory[allNodes];
		messageOutAddressMap = new ConcurrentHashMap<InetAddress, ConcurrentMap<MessageOut,InetAddress>>();
		messageInAddressMap = new ConcurrentHashMap<MessageIn, InetAddress>();
		addressSet = new HashSet<InetAddress>();
		countDowns = new ConcurrentHashMap<InetAddress, CountDownLatch>();
		for (int i = 0; i < allNodes; ++i) {
			localAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			broadcastAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			addressSet.add(broadcastAddresses[i]);
			messageOutAddressMap.put(broadcastAddresses[i], new ConcurrentHashMap<MessageOut, InetAddress>());
			rpcAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			heartBeats[i] = new HeartBeatState((int) System.currentTimeMillis());
			states[i] = new EndpointState(heartBeats[i]);
			valueFactories[i] = new VersionedValueFactory(partitioner);
            states[i].addApplicationState(ApplicationState.DC, valueFactories[i].datacenter(localDataCenter));
            states[i].addApplicationState(ApplicationState.HOST_ID, valueFactories[i].hostId(UUID.randomUUID()));
            states[i].addApplicationState(ApplicationState.NET_VERSION, valueFactories[i].networkVersion());
            states[i].addApplicationState(ApplicationState.RPC_ADDRESS, valueFactories[i].rpcaddress(rpcAddresses[i]));
            states[i].addApplicationState(ApplicationState.SCHEMA, valueFactories[i].schema(UUID.fromString("59adb24e-f3cd-3e02-97f0-5b395827453f")));
            TokenMetadata tokenMetadata = new TokenMetadata();
            Collection<Token> tokens = BootStrapper.getRandomTokens(tokenMetadata, numTokens);
            states[i].addApplicationState(ApplicationState.STATUS, valueFactories[i].normal(tokens));
            states[i].addApplicationState(ApplicationState.TOKENS, valueFactories[i].tokens(tokens));
            states[i].addApplicationState(ApplicationState.LOAD, valueFactories[i].load(10000));
		}
//		broadcastAddress
		endpointStateMapMap = new ConcurrentHashMap<InetAddress, ConcurrentMap<InetAddress,EndpointState>>();
		for (int i = 0; i < allNodes; ++i) {
            ConcurrentHashMap<InetAddress, EndpointState> endpointStateMaps = new ConcurrentHashMap<InetAddress, EndpointState>();
            endpointStateMaps.put(localAddresses[i], states[i]);
//            for (int j = 0; j < allNodes; ++j) {
//                endpointStateMaps.put(localAddresses[j], states[j]);
//            }
            endpointStateMapMap.put(broadcastAddresses[i], endpointStateMaps);
            logger.info(i + " " + endpointStateMaps.keySet());
		}

		// Gossiper.makeRandomGossipDigest
        Random random = new Random();
//        List<GossipDigest>[] gossipDigestLists = new List[allNodes];
//        MessageOut<GossipDigestSyn>[] messages = new MessageOut[allNodes];
        for (int i = 0; i < allNodes; ++i) {
//        	gossipDigestLists[i] = new ArrayList<GossipDigest>();
//            EndpointState epState;
//            int generation = 0;
//            int maxVersion = 0;
//            List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMapMap.get(broadcastAddresses[i]).keySet());
//            Collections.shuffle(endpoints, random);
//            for (InetAddress endpoint : endpoints)
//            {
//                epState = endpointStateMapMap.get(broadcastAddresses[i]).get(endpoint);
//                if (epState != null)
//                {
//                    generation = epState.getHeartBeatState().getGeneration();
//                    maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
//                }
//                gossipDigestLists[i].add(new GossipDigest(endpoint, generation, maxVersion));
//            }
//            GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestLists[i]);
//            messages[i] = new MessageOut<GossipDigestSyn>(broadcastAddresses[i], MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
            MessagingService.instance().listen(localAddresses[i]);
        }
        seed = InetAddress.getByName("127.0.0.1");
        messageOutAddressMap.put(seed, new ConcurrentHashMap<MessageOut, InetAddress>());
        target = InetAddress.getByName("127.0.0.2");
        messageOutAddressMap.put(target, new ConcurrentHashMap<MessageOut, InetAddress>());
        while (true) {
            currentNode = 0;

        	while (true) {
                List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
                EndpointState epState;
                int generation = 0;
                int maxVersion = 0;
                List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMapMap.get(broadcastAddresses[currentNode]).keySet());
                Collections.shuffle(endpoints, random);
                for (InetAddress endpoint : endpoints)
                {
                    epState = endpointStateMapMap.get(broadcastAddresses[currentNode]).get(endpoint);
                    if (epState != null)
                    {
                        generation = epState.getHeartBeatState().getGeneration();
                        maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
                    }
                    gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
                }
                GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestList);
                MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddresses[currentNode], MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
                heartBeats[currentNode].updateHeartBeat();
//                messageOutAddressMap.put(messages[currentNode], broadcastAddresses[currentNode]);
                int r = random.nextInt(allNodes + 2);
                if (r == 0) {
//                	logger.info("Put message " + messages[currentNode].hashCode() + " to seed by " + broadcastAddresses[currentNode]);
                	messageOutAddressMap.get(seed).put(message, broadcastAddresses[currentNode]);
                    MessagingService.instance().sendOneWay(message, seed);
                } else if (r == 1) {
//                	logger.info("Put message " + messages[currentNode].hashCode() + " to target by " + broadcastAddresses[currentNode]);
                	messageOutAddressMap.get(target).put(message, broadcastAddresses[currentNode]);
                    MessagingService.instance().sendOneWay(message, target);
//                	messageOutAddressMap.get(seed).put(messages[currentNode], broadcastAddresses[currentNode]);
//                    MessagingService.instance().sendOneWay(messages[currentNode], seed);
                } else {
//                	logger.info("Put message " + messages[currentNode].hashCode() + " to " + broadcastAddresses[r - 2] + " by " + broadcastAddresses[currentNode]);
//                	CountDownLatch countDown = new CountDownLatch(1);
//                	countDowns.put(broadcastAddresses[currentNode], countDown);
//                	messageOutAddressMap.get(seed).put(messages[currentNode], broadcastAddresses[currentNode]);
//                    MessagingService.instance().sendOneWay(messages[currentNode], seed);
//                    countDown.await();
//                    countDowns.remove(broadcastAddresses[currentNode]);
                	MessageIn<GossipDigestSyn> msgIn = convertOutToIn(message);
                	messageInAddressMap.put(msgIn, broadcastAddresses[r - 2]);
                	MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(msgIn, 
                			Integer.toString(idGen.incrementAndGet()));
                }
                currentNode = (currentNode + 1) % allNodes;
                if (currentNode == 0) {
                	break;
                }
        	}
        	Thread.sleep(1000);
        }
        
	}
	
	public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
		MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
		return msgIn;
	}

}
