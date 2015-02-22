package edu.uchicago.cs.ucare;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorstCaseGossiperStub {
	
    private static final Logger logger = LoggerFactory.getLogger(WorstCaseGossiperStub.class);

    public static InetAddress[] broadcastAddresses;
//	public static ConcurrentMap<InetAddress, EndpointState>[] endpointStateMaps;
	public static ConcurrentMap<InetAddress, ConcurrentMap<InetAddress, EndpointState>> endpointStateMapMap;
	public static ConcurrentMap<MessageOut, InetAddress> messageOutAddressMap;
	public static ConcurrentMap<MessageIn, InetAddress> messageInAddressMap;
	public static int currentNode;
//	public static int currentGroup;

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
		messageOutAddressMap = new ConcurrentHashMap<MessageOut, InetAddress>();
		messageInAddressMap = new ConcurrentHashMap<MessageIn, InetAddress>();
		for (int i = 0; i < allNodes; ++i) {
			localAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
			broadcastAddresses[i] = InetAddress.getByName(addressPrefix + (i + 3));
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
		}
		endpointStateMapMap = new ConcurrentHashMap<InetAddress, ConcurrentMap<InetAddress,EndpointState>>();
		for (int i = 0; i < allNodes; ++i) {
            ConcurrentHashMap<InetAddress, EndpointState> endpointStateMaps = new ConcurrentHashMap<InetAddress, EndpointState>();
            for (int j = 0; j < allNodes; ++j) {
                endpointStateMaps.put(localAddresses[j], states[j]);
            }
            endpointStateMapMap.put(broadcastAddresses[i], endpointStateMaps);
		}

		// Gossiper.makeRandomGossipDigest
        Random random = new Random();
        List<GossipDigest>[] gossipDigestLists = new List[allNodes];
        MessageOut<GossipDigestSyn>[] messages = new MessageOut[allNodes];
        for (int i = 0; i < allNodes; ++i) {
        	gossipDigestLists[i] = new ArrayList<GossipDigest>();
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
                gossipDigestLists[i].add(new GossipDigest(endpoint, generation, maxVersion));
            }
            GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster", "org.apache.cassandra.dht.Murmur3Partitioner", gossipDigestLists[i]);
            messages[i] = new MessageOut<GossipDigestSyn>(InetAddress.getByName("127.0.0." + (i + 3)), MessagingService.Verb.GOSSIP_DIGEST_SYN, digestSynMessage, GossipDigestSyn.serializer);
            messageOutAddressMap.put(messages[i], InetAddress.getByName("127.0.0." + (i + 3)));
            MessagingService.instance().listen(localAddresses[i]);
        }
        InetAddress seed = InetAddress.getByName("127.0.0.1");
        InetAddress target = InetAddress.getByName("127.0.0.2");
        logger.info("before send");
        while (true) {
            currentNode = 0;
        	while (true) {
                heartBeats[currentNode].updateHeartBeat();
                int r = random.nextInt(allNodes + 2);
                if (r == 0) {
                    MessagingService.instance().sendOneWay(messages[currentNode], seed);
                }
//                r = random.nextInt(allNodes + 2);
                if (r == 1) {
                	logger.info("node " + currentNode);
                    MessagingService.instance().sendOneWay(messages[currentNode], target);
                }
                currentNode = (currentNode + 1) % allNodes;
                if (currentNode == 0) {
                	break;
                }
        	}
        	Thread.sleep(1000);
        }
        
	}

}
