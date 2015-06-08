package edu.uchicago.cs.ucare.outdated;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    public static final AtomicInteger idGen = new AtomicInteger(0);
    public static InetAddress seed;
    public static InetAddress target;
    
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, ConfigurationException, InterruptedException {
        int currentNode;
		final int allNodes = 20;
		String localDataCenter = "datacenter1";
		int numTokens = 1024;
		String addressPrefix = "127.0.0.";
		InetAddress localAddresses[] = new InetAddress[allNodes];
		broadcastAddresses = new InetAddress[allNodes];
		InetAddress rpcAddresses[] = new InetAddress[allNodes];
		HeartBeatState[] heartBeats = new HeartBeatState[allNodes];
		final EndpointState[] states = new EndpointState[allNodes];
		IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
		final VersionedValueFactory[] valueFactories = new VersionedValueFactory[allNodes];
        final Collection<Token>[] tokens = new Collection[allNodes];
		addressSet = new HashSet<InetAddress>();
		for (int i = 0; i < allNodes; ++i) {
			localAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			broadcastAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			addressSet.add(broadcastAddresses[i]);
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
            tokens[i] = BootStrapper.getRandomTokens(tokenMetadata, numTokens);
		}
		endpointStateMapMap = new ConcurrentHashMap<InetAddress, ConcurrentMap<InetAddress,EndpointState>>();
		for (int i = 0; i < allNodes; ++i) {
            ConcurrentHashMap<InetAddress, EndpointState> endpointStateMaps = new ConcurrentHashMap<InetAddress, EndpointState>();
            endpointStateMaps.put(localAddresses[i], states[i]);
//            for (int j = 0; j < allNodes; ++j) {
//                endpointStateMaps.put(localAddresses[j], states[j]);
//            }
            endpointStateMapMap.put(broadcastAddresses[i], endpointStateMaps);
            logger.info(i + " " + endpointStateMaps.keySet());
            MessagingService.instance().listen(localAddresses[i]);
		}

		// Gossiper.makeRandomGossipDigest
        Random random = new Random();
        seed = InetAddress.getByName("127.0.0.1");
        target = InetAddress.getByName("127.0.0.2");
        Thread updateThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					Thread.sleep(5000);
					for (int i = 0; i < allNodes; ++i) {
                        states[i].addApplicationState(ApplicationState.TOKENS, valueFactories[i].tokens(tokens[i]));
                        states[i].addApplicationState(ApplicationState.STATUS, valueFactories[i].bootstrapping(tokens[i]));
					}
					Thread.sleep(5000);
					for (int i = 0; i < allNodes; ++i) {
                        states[i].addApplicationState(ApplicationState.TOKENS, valueFactories[i].tokens(tokens[i]));
                        states[i].addApplicationState(ApplicationState.STATUS, valueFactories[i].normal(tokens[i]));
                        states[i].addApplicationState(ApplicationState.SEVERITY, valueFactories[i].severity(0.0));
					}
					Thread.sleep(5000);
					for (int i = 0; i < allNodes; ++i) {
                        states[i].addApplicationState(ApplicationState.LOAD, valueFactories[i].load(10000));
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
        updateThread.start();
        for (int i = 0; i < allNodes; ++i) {
        	List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
            EndpointState epState;
            int generation = 0;
            int maxVersion = 0;
            List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMapMap.get(broadcastAddresses[i]).keySet());
            Collections.shuffle(endpoints, random);
            for (InetAddress endpoint : endpoints)
            {
                epState = endpointStateMapMap.get(broadcastAddresses[i]).get(endpoint);
                if (epState != null)
                {
                    generation = epState.getHeartBeatState().getGeneration();
                    maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
                }
                gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
            }
            GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestList);
            MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddresses[i], MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
            heartBeats[i].updateHeartBeat();
            MessagingService.instance().sendOneWay(message, seed);
//            Thread.sleep(3000);
//            System.out.println(endpointStateMapMap.get(broadcastAddresses[i]).size());
        }
//        Thread.sleep(2000);
//        System.exit(0);
        while (true) {
            currentNode = 0;

//        	while (true) {
//                List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
//                EndpointState epState;
//                int generation = 0;
//                int maxVersion = 0;
//                List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMapMap.get(broadcastAddresses[currentNode]).keySet());
//                Collections.shuffle(endpoints, random);
//                for (InetAddress endpoint : endpoints)
//                {
//                    epState = endpointStateMapMap.get(broadcastAddresses[currentNode]).get(endpoint);
//                    if (epState != null)
//                    {
//                        generation = epState.getHeartBeatState().getGeneration();
//                        maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
//                    }
//                    gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
//                }
//                GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestList);
//                MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddresses[currentNode], MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
//                heartBeats[currentNode].updateHeartBeat();
//                int r = random.nextInt(allNodes + 2);
//                if (r == 0) {
////                	logger.info("Put message " + message + " to seed by " + broadcastAddresses[currentNode]);
//                    MessagingService.instance().sendOneWay(message, seed);
//                } else if (r == 1) {
////                	logger.info("Put message " + message + " to target by " + broadcastAddresses[currentNode]);
//                    MessagingService.instance().sendOneWay(message, target);
//                    MessagingService.instance().sendOneWay(message, seed);
//                } else {
////                	logger.info("Put message " + message + " to " + broadcastAddresses[r - 2] + " by " + broadcastAddresses[currentNode]);
//                	for (; r == 0 || r == 1 || (r - 2) != currentNode; r = random.nextInt(allNodes + 2));
//                	MessageIn<GossipDigestSyn> msgIn = convertOutToIn(message);
//                	msgIn.setTo(broadcastAddresses[r - 2]);
//                	MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_SYN).doVerb(msgIn, 
//                			Integer.toString(idGen.incrementAndGet()));
//                    MessagingService.instance().sendOneWay(message, seed);
//                }
//                currentNode = (currentNode + 1) % allNodes;
//                if (currentNode == 0) {
//                	break;
//                }
//        	}
            int r = 19;
            for (int j = 0; j < 5; ++j) {
//            	r = random.nextInt(allNodes);
            	List<GossipDigest> gossipDigestList = new LinkedList<GossipDigest>();
                EndpointState epState;
                int generation = 0;
                int maxVersion = 0;
                List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMapMap.get(broadcastAddresses[r]).keySet());
                Collections.shuffle(endpoints, random);
                for (InetAddress endpoint : endpoints)
                {
                    epState = endpointStateMapMap.get(broadcastAddresses[r]).get(endpoint);
                    if (epState != null)
                    {
                        generation = epState.getHeartBeatState().getGeneration();
                        maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
                    }
                    gossipDigestList.add(new GossipDigest(endpoint, generation, maxVersion));
                }
                GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestList);
                MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(broadcastAddresses[r], MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
                heartBeats[r].updateHeartBeat();
                MessagingService.instance().sendOneWay(message, target);
                r = (r - 1);
                r = r < 0 ? 19 : r;
            }
        	Thread.sleep(1000);
//            for (int i = 0; i < allNodes; ++i) {
//                System.out.println(endpointStateMapMap.get(broadcastAddresses[i]).size());
//            }
        }
        
	}
	
	public static <T> MessageIn<T> convertOutToIn(MessageOut<T> msgOut) {
		MessageIn<T> msgIn = MessageIn.create(msgOut.from, msgOut.payload, msgOut.parameters, msgOut.verb, MessagingService.VERSION_12);
		return msgIn;
	}

}
