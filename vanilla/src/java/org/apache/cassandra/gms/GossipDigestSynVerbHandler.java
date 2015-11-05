/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import edu.uchicago.cs.ucare.util.Klogger;

public class GossipDigestSynVerbHandler implements IVerbHandler<GossipDigestSyn>
{
    private static final Logger logger = LoggerFactory.getLogger( GossipDigestSynVerbHandler.class);

    public void doVerb(MessageIn<GossipDigestSyn> message, String id)
    {
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
            return;
        }

        GossipDigestSyn gDigestMessage = message.payload;
        /* If the message is from a different cluster throw it away. */
        if (!gDigestMessage.clusterId.equals(DatabaseDescriptor.getClusterName()))
        {
            logger.warn("ClusterName mismatch from " + from + " " + gDigestMessage.clusterId  + "!=" + DatabaseDescriptor.getClusterName());
            return;
        }

        if (gDigestMessage.partioner != null && !gDigestMessage.partioner.equals(DatabaseDescriptor.getPartitionerName()))
        {
            logger.warn("Partitioner mismatch from " + from + " " + gDigestMessage.partioner  + "!=" + DatabaseDescriptor.getPartitionerName());
            return;
        }

        List<GossipDigest> gDigestList = gDigestMessage.getGossipDigests();
        if (logger.isTraceEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for ( GossipDigest gDigest : gDigestList )
            {
                sb.append(gDigest);
                sb.append(" ");
            }
            logger.trace("Gossip syn digests are : " + sb.toString());
        }
        StringBuilder sb = new StringBuilder();
        for ( GossipDigest gDigest : gDigestList )
        {
            sb.append(gDigest);
            sb.append(", ");
        }
//        Klogger.logger.info("GDS digests from " + from + " are (" + sb.toString() + ")");
        
        long start;
        long end;

        start = System.currentTimeMillis();
        doSort(gDigestList);
        end = System.currentTimeMillis();
        long doSort = end - start;

        List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        start = System.currentTimeMillis();
        Gossiper.instance.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap);
        sb = new StringBuilder();
        for (GossipDigest d : deltaGossipDigestList) {
            sb.append(d.getEndpoint());
            sb.append(',');
        }
        Klogger.logger.info("delta digest list = " + sb.toString());
        Klogger.logger.info("delta ep state map = " + deltaEpStateMap.keySet());
        end = System.currentTimeMillis();
        long examine = end - start;

        MessageOut<GossipDigestAck> gDigestAckMessage = new MessageOut<GossipDigestAck>(MessagingService.Verb.GOSSIP_DIGEST_ACK,
                                                                                                      new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap),
                                                                                                      GossipDigestAck.serializer);
        /*
        for (InetAddress address : deltaEpStateMap.keySet()) {
        	EndpointState eps = deltaEpStateMap.get(address);
        	Map<ApplicationState, VersionedValue> appStateMap = eps.getApplicationStateMap();
            StringBuilder strBuilder = new StringBuilder();
        	for (ApplicationState state : appStateMap.keySet()) {
        		VersionedValue value = appStateMap.get(state);
        		strBuilder.append(state + "=" + (state == ApplicationState.TOKENS ? "Length(" + value.value.length() + ")," + value.version + ")" : value) + ", ");
        	}
//            Klogger.logger.info("Sending GDA to " + from + " about node " + address + " with content (" + strBuilder.toString() + ")"); 
        }
        sb = new StringBuilder();
        for (GossipDigest gDigest : deltaGossipDigestList)
        {
            sb.append(gDigest);
            sb.append(", ");
        }
//        Klogger.logger.info("GDA digests to " + from + " are (" + sb.toString() + ") with size " + gDigestAckMessage.serializedSize(MessagingService.current_version) + " bytes");
        */
//        if (deltaEpStateMap.keySet())
        for (InetAddress observedNode : FailureDetector.observedNodes) {
        	if (deltaEpStateMap.keySet().contains(observedNode)) {
        		int version = Gossiper.getMaxEndpointStateVersion(deltaEpStateMap.get(observedNode));
        		Klogger.logger.info("propagate info of " + observedNode + " to " + from + " version " + version);
        	}
        }
        Klogger.logger.info("GDA to " + from + " has size " + gDigestAckMessage.serializedSize(MessagingService.current_version) + " bytes");
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        Gossiper.instance.checkSeedContact(from);
        start = System.currentTimeMillis();
        MessagingService.instance().sendOneWay(gDigestAckMessage, from);
        end = System.currentTimeMillis();
        long send = end - start;
        Klogger.logger.info("SyncHandler for " + from + " doSort took {} ms, examine took {} ms, sendMsg took {} ms", doSort, examine, send);
    }

    /*
     * First construct a map whose key is the endpoint in the GossipDigest and the value is the
     * GossipDigest itself. Then build a list of version differences i.e difference between the
     * version in the GossipDigest and the version in the local state for a given InetAddress.
     * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
     * to the endpoint from the map that was initially constructed.
    */
    private void doSort(List<GossipDigest> gDigestList)
    {
        /* Construct a map of endpoint to GossipDigest. */
        Map<InetAddress, GossipDigest> epToDigestMap = new HashMap<InetAddress, GossipDigest>();
        for ( GossipDigest gDigest : gDigestList )
        {
            epToDigestMap.put(gDigest.getEndpoint(), gDigest);
        }

        /*
         * These digests have their maxVersion set to the difference of the version
         * of the local EndpointState and the version found in the GossipDigest.
        */
        List<GossipDigest> diffDigests = new ArrayList<GossipDigest>(gDigestList.size());
        for ( GossipDigest gDigest : gDigestList )
        {
            InetAddress ep = gDigest.getEndpoint();
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
            int version = (epState != null) ? Gossiper.instance.getMaxEndpointStateVersion( epState ) : 0;
            int diffVersion = Math.abs(version - gDigest.getMaxVersion() );
            diffDigests.add( new GossipDigest(ep, gDigest.getGeneration(), diffVersion) );
        }

        gDigestList.clear();
        Collections.sort(diffDigests);
        int size = diffDigests.size();
        /*
         * Report the digests in descending order. This takes care of the endpoints
         * that are far behind w.r.t this local endpoint
        */
        for ( int i = size - 1; i >= 0; --i )
        {
            gDigestList.add( epToDigestMap.get(diffDigests.get(i).getEndpoint()) );
        }
    }
}
