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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.OneMachineScaleSimulator;

public class SimulatedGossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAckVerbHandler.class);
    
    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        InetAddress from = message.from;
        InetAddress to = message.to;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
//        if (!Gossiper.instance.isEnabled())
//        {
//            if (logger.isTraceEnabled())
//                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
//            return;
//        }

        GossipDigestAck gDigestAckMessage = message.payload;
        int ackHash = gDigestAckMessage.hashCode();
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        
        GossiperStub stub = OneMachineScaleSimulator.stubGroup.getStub(to);

        if (stub.getHasContactedSeed() && !OneMachineScaleSimulator.isTestNodesStarted && from.equals(OneMachineScaleSimulator.seed)) {
            GossipDigest digestForStub = null;
            for (GossipDigest gDigest : gDigestList) {
                if (gDigest.endpoint.equals(to)) {
                    digestForStub = gDigest;
                }
            }
            assert digestForStub != null;
            List<GossipDigest> deltaGossipDigestList = new ArrayList<GossipDigest>();
            Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
            Gossiper.examineGossiperStatic(stub.getEndpointStateMap(), digestForStub, deltaGossipDigestList, deltaEpStateMap);
            MessageOut<GossipDigestAck2> gDigestAck2Message = new MessageOut<GossipDigestAck2>(
                   to, MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                   new GossipDigestAck2(deltaEpStateMap),
                   GossipDigestAck2.serializer);
            Gossiper.instance.checkSeedContact(from);
            MessagingService.instance().sendOneWay(gDigestAck2Message, from);
            return;
        }
        
        int newNode = 0;
        int newNodeToken = 0;
        int newRestart = 0;
        int newVersion = 0;
        int newVersionToken = 0;
        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
        if ( epStateMap.size() > 0 )
        {
            for (InetAddress observedNode : OneMachineScaleSimulator.testNodes) {
                if (epStateMap.keySet().contains(observedNode)) {
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
                        logger.info("receive info of " + observedNode + " from " + from + 
                                " generation " + remoteGen + " version " + remoteVersion);
                        newerVersion.put(observedNode, remoteVersion);
                    }
                }
            }
            /* Notify the Failure Detector */
//            Gossiper.instance.notifyFailureDetector(epStateMap);
//            Gossiper.instance.applyStateLocally(epStateMap);
            Gossiper.notifyFailureDetectorStatic(stub.getEndpointStateMap(), epStateMap);
            Gossiper.applyStateLocallyStatic(stub, epStateMap);

            for (InetAddress address : epStateMap.keySet()) {
                if (OneMachineScaleSimulator.testNodes.contains(address)) {
                    // Implement here
                    OneMachineScaleSimulator.startForwarding(from, address, to);
                    break;
//                    EndpointState epState = epStateMap.get(address);
//                    ScaleSimulator.stubGroup.getOmniscientGossiperStub().addClockEndpointStateIfNotExist(address, epState);
                }
            }
            
        }

        Gossiper.instance.checkSeedContact(from);

        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        for( GossipDigest gDigest : gDigestList )
        {
            InetAddress addr = gDigest.getEndpoint();
//            EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            EndpointState localEpStatePtr = Gossiper.getStateForVersionBiggerThanStatic(stub.getEndpointStateMap(),
            		addr, gDigest.getMaxVersion());
            if ( localEpStatePtr != null )
                deltaEpStateMap.put(addr, localEpStatePtr);
        }

        MessageOut<GossipDigestAck2> gDigestAck2Message = new MessageOut<GossipDigestAck2>(
        		to, MessagingService.Verb.GOSSIP_DIGEST_ACK2,
               new GossipDigestAck2(deltaEpStateMap),
               GossipDigestAck2.serializer);
        int ack2Hash = gDigestAck2Message.payload.hashCode();
        for (InetAddress observedNode : OneMachineScaleSimulator.testNodes) {
            if (deltaEpStateMap.keySet().contains(observedNode)) {
                int version = Gossiper.getMaxEndpointStateVersion(deltaEpStateMap.get(observedNode));
                logger.info("propagate info of " + observedNode + " to " + from + " version " + version);
                logger.info("Receive ack:" + ackHash + " ; Send ack2:" + ack2Hash + 
                        " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                        " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                        " ; Forwarding " + observedNode + " to " + from + " version " + version);
            }
        }
        for (InetAddress address : newerVersion.keySet()) {
            logger.info("Receive ack:" + ackHash + " ; Send ack2:" + ack2Hash + 
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
            
        }
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        if (OneMachineScaleSimulator.stubGroup.contains(from)) {
            MessageIn<GossipDigestAck2> msgIn = OneMachineScaleSimulator.convertOutToIn(gDigestAck2Message);
            msgIn.setTo(from);
            long s = System.currentTimeMillis();
            MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_ACK2).doVerb(msgIn, 
                    Integer.toString(OneMachineScaleSimulator.idGen.incrementAndGet()));
            long t = System.currentTimeMillis() - s;
            logger.info("sc_debug: Doing verb \"" + Verb.GOSSIP_DIGEST_ACK2 + "\" from " + msgIn.from + " took " + t + " ms");
        } else {
            MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        }
//        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        if (!stub.getHasContactedSeed() && from.equals(OneMachineScaleSimulator.seed)) {
            synchronized (stub) {
                stub.setHasContactedSeed(true);
//                stub.notify();
            }
        }
    }
}
