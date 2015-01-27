package edu.uchicago.cs.ucare;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class GossiperStub {
	
	public static HeartBeatState heartBeatState;

	public static void main(String[] args) throws UnknownHostException, ConfigurationException {
//		Map<ApplicationState, VersionedValue> appStates = new HashMap<ApplicationState, VersionedValue>();
		heartBeatState = new HeartBeatState(((int) System.currentTimeMillis() / 1000));
		ConcurrentMap<InetAddress, EndpointState> endpointStateMap = new ConcurrentHashMap<InetAddress, EndpointState>();
		endpointStateMap.put(InetAddress.getByName("127.0.0.5"), new EndpointState(heartBeatState));
		heartBeatState.updateHeartBeat();
		
		// Gossiper.makeRandomGossipDigest
		final List<GossipDigest> gDigests = new ArrayList<GossipDigest>();
		Random random = new Random();
		EndpointState epState;
        int generation = 0;
        int maxVersion = 0;
        List<InetAddress> endpoints = new ArrayList<InetAddress>(endpointStateMap.keySet());
        Collections.shuffle(endpoints, random);
        System.out.println(endpointStateMap);
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
        
        System.out.println(gDigests.toString());

        if ( gDigests.size() > 0 )
        {
        	System.out.println("korn 1");
            GossipDigestSyn digestSynMessage = new GossipDigestSyn(DatabaseDescriptor.getClusterName(),
                                                                   DatabaseDescriptor.getPartitionerName(),
                                                                   gDigests);
        	System.out.println("korn 2");
            MessageOut<GossipDigestSyn> message = new MessageOut<GossipDigestSyn>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                                                digestSynMessage,
                                                                                                GossipDigestSyn.serializer);
        	System.out.println("korn 3");
            MessagingService.instance().listen(InetAddress.getByName("127.0.0.5"));
            MessagingService.instance().sendOneWay(message, InetAddress.getByName("127.0.0.1"));
        	System.out.println("korn 4");
        }
        
        
        
//        Gossiper.instance.makeRandomGossipDigest(gDigests);
//		Gossiper.instance.start(SystemTable.incrementAndGetGeneration(), appStates);
	}

}
