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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import edu.uchicago.cs.ucare.util.Klogger;

public class GossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAckVerbHandler.class);

    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
        InetAddress from = message.from;
        InetAddress to = FBUtilities.getBroadcastAddress();
    	long start;
    	long end;
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
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        
        long notifyFD = 0;
        long applyState = 0;
        int newNode = 0;
        int newNodeToken = 0;
        int newRestart = 0;
        int newVersion = 0;
        int newVersionToken = 0;
        int bootstrapCount = 0;
        int normalCount = 0;
        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
        if ( epStateMap.size() > 0 )
        {
            for (InetAddress observedNode : epStateMap.keySet()) {
                EndpointState localEpState = Gossiper.instance.getEndpointStateForEndpoint(observedNode);
                EndpointState remoteEpState = epStateMap.get(observedNode);
                int remoteGen = remoteEpState.getHeartBeatState().getGeneration();
                int remoteVersion = Gossiper.getMaxEndpointStateVersion(remoteEpState);
                boolean newer = false;
                if (localEpState == null) {
                    newer = true;
                } else {
                    synchronized (localEpState) {
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
                }
                if (newer) {
//                    double hbAverage = 0;
//                    FailureDetector fd = (FailureDetector) FailureDetector.instance;
//                    if (fd.arrivalSamples.containsKey(observedNode)) {
//                        hbAverage = fd.arrivalSamples.get(observedNode).mean();
//                    }
//                    Klogger.logger.info("receive info of " + observedNode + " from " + from + 
//                            " generation " + remoteGen + " version " + remoteVersion + " gossip_average " + hbAverage);
                    newerVersion.put(observedNode, remoteVersion);
                }
            }
            
            /* Notify the Failure Detector */
        	start = System.currentTimeMillis();
            Gossiper.instance.notifyFailureDetector(epStateMap);
            end = System.currentTimeMillis();
            notifyFD = end - start;
        	start = System.currentTimeMillis();
            Object[] result = Gossiper.instance.applyStateLocally(epStateMap);
            end = System.currentTimeMillis();
            newNode = (int) result[0];
            newNodeToken = (int) result[1];
            newRestart = (int) result[2];
            newVersion = (int) result[3];
            newVersionToken = (int) result[4];
            bootstrapCount = (int) result[5];
            normalCount = (int) result[6];
            Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
            applyState = end - start;
            for (InetAddress receivingAddress : updatedNodes) {
                EndpointState ep = Gossiper.instance.endpointStateMap.get(receivingAddress);
                Klogger.logger.info(to + " is hop " + ep.hopNum + " for " + receivingAddress + " with version " + ep.getHeartBeatState().getHeartBeatVersion() + " from " + from);
            }
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
        Map<InetAddress, EndpointState> localEpStateMap = Gossiper.instance.endpointStateMap;
        for (InetAddress sendingAddress : deltaEpStateMap.keySet()) {
            deltaEpStateMap.get(sendingAddress).setHopNum(localEpStateMap.get(sendingAddress).hopNum);
        }

        MessageOut<GossipDigestAck2> gDigestAck2Message = 
                new MessageOut<GossipDigestAck2>(MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                     new GossipDigestAck2(deltaEpStateMap, message.payload.syncId, message.payload.msgId),
                     GossipDigestAck2.serializer);
//        long sendTime = System.currentTimeMillis();
//        for (InetAddress observedNode : deltaEpStateMap.keySet()) {
//            int version = Gossiper.getMaxEndpointStateVersion(deltaEpStateMap.get(observedNode));
//            Klogger.logger.info("Receive ack:" + receiveTime + " (" + (notifyFD + applyState + examine) + "ms)" + " ; Send ack2:" + sendTime + 
//                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
//                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
//                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
//                    " ; Forwarding " + observedNode + " to " + from + " version " + version);
//        }
//        for (InetAddress address : newerVersion.keySet()) {
//            Klogger.logger.info("Receive ack:" + receiveTime + " (" + (notifyFD + applyState + examine) + "ms)" + " ; Send ack2:" + sendTime + 
//                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
//                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
//                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
//                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
//            
//        }
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        start = System.currentTimeMillis();
        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        end = System.currentTimeMillis();
        long send = end - start;
        long ackHandlerTime = System.currentTimeMillis() - receiveTime;
        Klogger.logger.info(to + " executes gossip_ack took " + ackHandlerTime + " ms");
        if (bootstrapCount != 0 || normalCount != 0) {
            Klogger.logger.info(to + " apply gossip_ack boot " + bootstrapCount + " normal " + normalCount);
        }
        Klogger.logger.info("AckHandler for " + from + " notifyFD took {} ms, applyState took {} ms, examine took {} ms, sendMsg took {} ms", notifyFD, applyState, examine, send);
    }
}
