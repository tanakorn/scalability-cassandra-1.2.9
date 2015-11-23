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

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator;

public class SimulatedGossipDigestSynVerbHandler implements IVerbHandler<GossipDigestSyn>
{
    private static final Logger logger = LoggerFactory.getLogger( SimulatedGossipDigestSynVerbHandler.class);
    private static final Map<String, byte[]> emptyMap = Collections.<String, byte[]>emptyMap();

    public void doVerb(MessageIn<GossipDigestSyn> message, String id)
    {
        InetAddress from = message.from;
        InetAddress to = message.to;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestSynMessage from {}", from);
//        if (!Gossiper.instance.isEnabled())
//        {
//            if (logger.isTraceEnabled())
//                logger.trace("Ignoring GossipDigestSynMessage because gossip is disabled");
//            return;
//        }

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
        GossiperStub stub = WholeClusterSimulator.stubGroup.getStub(to);
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
            sb.append(" ");
        }
        logger.debug(stub + " receieve syn digests : " + sb.toString());

        doSort(gDigestList);

        List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        // I let this got executed because it's not expensive
        Gossiper.examineGossiperStatic(stub, stub.getEndpointStateMap(), gDigestList, deltaGossipDigestList, deltaEpStateMap);
        logger.debug(stub + " doesn't know about " + deltaGossipDigestList.toString());

        MessageIn<GossipDigestAck> gDigestAckMessage = MessageIn.create(to, 
                new GossipDigestAck(deltaGossipDigestList, deltaEpStateMap), emptyMap, 
                MessagingService.Verb.GOSSIP_DIGEST_ACK, MessagingService.VERSION_12);
        int bootNodeNum = 0;
        int normalNodeNum = 0;
        for (InetAddress address : deltaEpStateMap.keySet()) {
            EndpointState ep = deltaEpStateMap.get(address);
            for (ApplicationState appState : ep.applicationState.keySet()) {
                if (appState == ApplicationState.STATUS) {
                    VersionedValue value = ep.applicationState.get(appState);
                    String apStateValue = value.value;
                    String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
                    assert (pieces.length > 0);
                    String moveName = pieces[0];
                    if (moveName.equals(VersionedValue.STATUS_BOOTSTRAPPING)) {
                        bootNodeNum++;
                    } else if (moveName.equals(VersionedValue.STATUS_NORMAL)) {
                        normalNodeNum++;
                    }
                }
            }
        }
        if (bootNodeNum == 128) {
            bootNodeNum = 127;
        }
        if (normalNodeNum == 128) {
            normalNodeNum = 127;
        }
        long sleepTime = WholeClusterSimulator.bootGossipExecRecords[bootNodeNum] + 
                WholeClusterSimulator.normalGossipExecRecords[normalNodeNum];
//        System.out.println("should sleep " + sleepTime);
        long wakeUpTime = System.currentTimeMillis() + sleepTime;
        gDigestAckMessage.setWakeUpTime(wakeUpTime);
        gDigestAckMessage.setTo(from);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAckMessage to {}", from);
        // TODO Can I comment this out?
        Gossiper.instance.checkSeedContact(from);
        WholeClusterSimulator.ackQueue.add(gDigestAckMessage);
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
