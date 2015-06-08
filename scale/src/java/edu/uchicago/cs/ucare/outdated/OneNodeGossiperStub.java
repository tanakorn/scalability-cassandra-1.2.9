package edu.uchicago.cs.ucare.outdated;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class OneNodeGossiperStub {
	
	public static HeartBeatState heartBeatState;
	public static EndpointState state;

	public static void main(String[] args) throws UnknownHostException, ConfigurationException, InterruptedException {
//		Map<ApplicationState, VersionedValue> appStates = new HashMap<ApplicationState, VersionedValue>();
//		appStates.put(key, value)
		heartBeatState = new HeartBeatState(((int) System.currentTimeMillis()));
		ConcurrentMap<InetAddress, EndpointState> endpointStateMap = new ConcurrentHashMap<InetAddress, EndpointState>();
		state = new EndpointState(heartBeatState);
		IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
		VersionedValueFactory valueFactory = new VersionedValueFactory(partitioner);
		state.addApplicationState(ApplicationState.DC, valueFactory.datacenter(DatabaseDescriptor.getLocalDataCenter()));
		state.addApplicationState(ApplicationState.HOST_ID, valueFactory.hostId(UUID.randomUUID()));
		state.addApplicationState(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(DatabaseDescriptor.getRpcAddress()));
		state.addApplicationState(ApplicationState.SCHEMA, valueFactory.schema(UUID.fromString("59adb24e-f3cd-3e02-97f0-5b395827453f")));
		TokenMetadata tokenMetadata = new TokenMetadata();
		Collection<Token> tokens = BootStrapper.getRandomTokens(tokenMetadata, DatabaseDescriptor.getNumTokens());
		state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
		state.addApplicationState(ApplicationState.NET_VERSION, valueFactory.networkVersion());
		state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
		endpointStateMap.put(FBUtilities.getLocalAddress(), state);

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
            GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                                                                   DatabaseDescriptor.getPartitionerName(),
                                                                   gDigests);
            MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                                digestSynMessage,
                                                                                                GossipDigestSyn.serializer);
            MessagingService.instance().listen(FBUtilities.getLocalAddress());
            while (true) {
                heartBeatState.updateHeartBeat();
                // real cassandra node listen on address 127.0.0.1
                MessagingService.instance().sendOneWay(message, InetAddress.getByName("127.0.0.1"));
                Thread.sleep(1000);
            }
        }

	}

}
