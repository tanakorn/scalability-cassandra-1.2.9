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
import org.apache.cassandra.dht.ByteOrderedPartitioner;
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
import org.apache.cassandra.utils.FBUtilities;

public class TwoNodeGossiperStub {
	
	public static ConcurrentMap<InetAddress, EndpointState> endpointStateMap;
	
	public static String localDataCenter;
	public static HeartBeatState heartBeatState;
	public static EndpointState state;
	public static IPartitioner partitioner;
	public static InetAddress localAddress;
	public static InetAddress broadcastAddress;
	public static InetAddress rpcAddress;
	public static int numTokens = 4;

	public static void main(String[] args) throws UnknownHostException, ConfigurationException, InterruptedException {
		localDataCenter = "datacenter1";
		localAddress = InetAddress.getByName("127.0.0.5");
		broadcastAddress = InetAddress.getByName("127.0.0.5");
		rpcAddress = InetAddress.getByName("127.0.0.5");
		heartBeatState = new HeartBeatState(((int) System.currentTimeMillis()));
		endpointStateMap = new ConcurrentHashMap<InetAddress, EndpointState>();
		state = new EndpointState(heartBeatState);
		partitioner = DatabaseDescriptor.getPartitioner();
		VersionedValueFactory valueFactory = new VersionedValueFactory(partitioner);
		state.addApplicationState(ApplicationState.DC, valueFactory.datacenter(localDataCenter));
		state.addApplicationState(ApplicationState.HOST_ID, valueFactory.hostId(UUID.randomUUID()));
		state.addApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(rpcAddress));
		state.addApplicationState(ApplicationState.SCHEMA, valueFactory.schema(UUID.fromString("59adb24e-f3cd-3e02-97f0-5b395827453f")));
		TokenMetadata tokenMetadata = new TokenMetadata();
		Collection<Token> tokens = BootStrapper.getRandomTokens(tokenMetadata, numTokens);
		state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
		state.addApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
		state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
		endpointStateMap.put(localAddress, state);
		
		EndpointState state2 = new EndpointState(heartBeatState);
		VersionedValueFactory valueFactory2 = new VersionedValueFactory(DatabaseDescriptor.getPartitioner());
		state2.addApplicationState(ApplicationState.DC, valueFactory2.datacenter(localDataCenter));
		state2.addApplicationState(ApplicationState.HOST_ID, valueFactory2.hostId(UUID.randomUUID()));
		state2.addApplicationState(ApplicationState.RPC_ADDRESS, valueFactory2.rpcaddress(rpcAddress));
		state2.addApplicationState(ApplicationState.SCHEMA, valueFactory2.schema(UUID.fromString("59adb24e-f3cd-3e02-97f0-5b395827453f")));
		TokenMetadata tokenMetadata2 = new TokenMetadata();
		Collection<Token> tokens2 = BootStrapper.getRandomTokens(tokenMetadata2, numTokens);
		state2.addApplicationState(ApplicationState.STATUS, valueFactory2.normal(tokens2));
		state2.addApplicationState(ApplicationState.NET_VERSION, valueFactory2.networkVersion());
		state2.addApplicationState(ApplicationState.TOKENS, valueFactory2.tokens(tokens2));
		endpointStateMap.put(InetAddress.getByName("127.0.0.2"), state2);

		// Gossiper.makeRandomGossipDigest
        final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
        Random random = new Random();
        EndpointState epState;
        int generation = 0;
        int maxVersion = 0;
        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
//        System.out.println(endpointStateMap);
        for (InetAddress endpoint : endpoints)
        {
            epState = endpointStateMap.get(endpoint);
            if (epState != null)
            {
                generation = epState.getHeartBeatState().getGeneration();
                maxVersion = epState.getHeartBeatState().getHeartBeatVersion();
            }
            gDigests.add(new GossipDigest(endpoint, generation, maxVersion));
        }
//        System.out.println(gDigests.toString());
        
        if ( gDigests.size() > 0 )
        {
            GossipDigestSyn digestSynMessage = new GossipDigestSyn("Test Cluster",
                                                                   "org.apache.cassandra.dht.Murmur3Partitioner",
                                                                   gDigests);
            MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                                digestSynMessage,
                                                                                                GossipDigestSyn.serializer);
            MessagingService.instance().listen(FBUtilities.getLocalAddress());
            MessagingService.instance().listen(InetAddress.getByName("127.0.0.2"));
            while (true) {
                heartBeatState.updateHeartBeat();
                // real cassandra node listen on address 127.0.0.1
                MessagingService.instance().sendOneWay(message, InetAddress.getByName("127.0.0.1"));
                Thread.sleep(1000);
            }
        }

	}

}
