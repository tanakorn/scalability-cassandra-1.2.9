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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class GossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAckVerbHandler.class);

    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
    	long start;
    	long end;
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
            return;
        }

        GossipDigestAck gDigestAckMessage = message.payload;
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        /*
        StringBuilder sb = new StringBuilder();
        for ( GossipDigest gDigest : gDigestList )
        {
            sb.append(gDigest);
            sb.append(", ");
        }
//        logger.info("sc_debug: GDA digests from " + from + " are (" + sb.toString() + ")");
        */
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();

        /*
        for (InetAddress address : epStateMap.keySet()) {
        	EndpointState eps = epStateMap.get(address);
        	Map<ApplicationState, VersionedValue> appStateMap = eps.getApplicationStateMap();
            StringBuilder strBuilder = new StringBuilder();
            int maxVersion = 0;
        	for (ApplicationState state : appStateMap.keySet()) {
        		VersionedValue value = appStateMap.get(state);
        		if (value.version > maxVersion) {
        			maxVersion = value.version;
        		}
//        		strBuilder.append(state + "=" + (state == ApplicationState.TOKENS ? "Length(" + value.value.length() + ")," + value.version + ")" : value) + ", ");
        	}
//            logger.info("sc_debug: Reading GDA from " + from + " about node " + address + " with content (" + strBuilder.toString() + ")"); 
            logger.info("sc_debug: Reading GDA from " + from + " about node " + address + " with version " + maxVersion);
        }
        */
        

        long notifyFD = 0;
        long applyState = 0;
        if ( epStateMap.size() > 0 )
        {
            for (InetAddress observedNode : FailureDetector.observedNodes) {
                if (epStateMap.keySet().contains(observedNode)) {
                    EndpointState localEpState = Gossiper.instance.getEndpointStateForEndpoint(observedNode);
                    synchronized (localEpState) {
                        EndpointState remoteEpState = epStateMap.get(observedNode);
                        int remoteGen = remoteEpState.getHeartBeatState().getGeneration();
                        int remoteVersion = Gossiper.getMaxEndpointStateVersion(remoteEpState);
                        boolean newer = false;
                        if (localEpState == null) {
                            newer = true;
                        } else {
                            int localGen = localEpState.getHeartBeatState().getGeneration();
                            if (localGen < remoteGen) {
                                newer = true;
                            } else if (localGen == remoteGen) {
                                int localVersion = Gossiper.getMaxEndpointStateVersion(localEpState);
                                if (localVersion < remoteVersion) {
                                    newer = true;
                                }
                            }
                        }
                        if (newer) {
                            logger.info("sc_debug: receive info of " + observedNode + " from " + from + 
                                    " generation " + remoteGen + " version " + remoteVersion);
                        }
                    }
                }
            }
            /* Notify the Failure Detector */
        	start = System.currentTimeMillis();
            Gossiper.instance.notifyFailureDetector(epStateMap);
            end = System.currentTimeMillis();
            notifyFD = end - start;
        	start = System.currentTimeMillis();
            Gossiper.instance.applyStateLocally(epStateMap);
            end = System.currentTimeMillis();
            applyState = end - start;
        }

        Gossiper.instance.checkSeedContact(from);

        start = System.currentTimeMillis();
        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        for( GossipDigest gDigest : gDigestList )
        {
            InetAddress addr = gDigest.getEndpoint();
            EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            if ( localEpStatePtr != null ) {
                deltaEpStateMap.put(addr, localEpStatePtr);
            }
        }
        end = System.currentTimeMillis();
        long examine = end - start;

        MessageOut<GossipDigestAck2> gDigestAck2Message = new MessageOut<GossipDigestAck2>(MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                                                                                                         new GossipDigestAck2(deltaEpStateMap),
                                                                                                         GossipDigestAck2.serializer);
        /*
        for (InetAddress address : deltaEpStateMap.keySet()) {
        	EndpointState eps = deltaEpStateMap.get(address);
        	Map<ApplicationState, VersionedValue> appStateMap = eps.getApplicationStateMap();
            StringBuilder strBuilder = new StringBuilder();
        	for (ApplicationState state : appStateMap.keySet()) {
        		VersionedValue value = appStateMap.get(state);
        		strBuilder.append(state + "=" + (state == ApplicationState.TOKENS ? "Length(" + value.value.length() + ")," + value.version + ")" : value) + ", ");
        	}
//            logger.info("sc_debug: Sending GDA2 to " + from + " about node " + address + " with content (" + strBuilder.toString() + ")"); 
        }
        sb = new StringBuilder();
        for ( GossipDigest gDigest : gDigestList )
        {
            sb.append(gDigest);
            sb.append(", ");
        }
        */
//        logger.info("sc_debug: GDA2 digests from " + from + " are (" + sb.toString() + ") with size" + gDigestAck2Message.serializedSize(MessagingService.current_version) + " bytes");
        for (InetAddress observedNode : FailureDetector.observedNodes) {
        	if (deltaEpStateMap.keySet().contains(observedNode)) {
        		int version = Gossiper.getMaxEndpointStateVersion(deltaEpStateMap.get(observedNode));
        		logger.info("sc_debug: propagate info of " + observedNode + " to " + from + " version " + version);
        	}
        }
        logger.info("sc_debug: GDA2 to " + from + " has size " + gDigestAck2Message.serializedSize(MessagingService.current_version) + " bytes");
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        start = System.currentTimeMillis();
        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        end = System.currentTimeMillis();
        long send = end - start;
        logger.info("sc_debug: AckHandler for " + from + " notifyFD took {} ms, applyState took {} ms, examine took {} ms, sendMsg took {} ms", notifyFD, applyState, examine, send);
    }
}
