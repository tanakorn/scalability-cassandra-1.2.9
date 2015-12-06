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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator;

public class SimulatedGossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAckVerbHandler.class);
    private static final Map<String, byte[]> emptyMap = Collections.<String, byte[]>emptyMap();
    
    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
    	long start;
    	long end;
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
        
        GossiperStub stub = WholeClusterSimulator.stubGroup.getStub(to);
        
        int epStateMapSize = epStateMap.size();

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
                EndpointState localEpState = stub.getEndpointStateMap().get(observedNode);
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
                    double hbAverage = 0;
                    FailureDetector fd = (FailureDetector) stub.failureDetector;
                    if (fd.arrivalSamples.containsKey(observedNode)) {
                        hbAverage = fd.arrivalSamples.get(observedNode).mean();
                    }
                    logger.info("receive info of " + observedNode + " from " + from + 
                            " generation " + remoteGen + " version " + remoteVersion + " gossip_average " + hbAverage);
                    newerVersion.put(observedNode, remoteVersion);
                }
            }
            /* Notify the Failure Detector */
//            Gossiper.instance.notifyFailureDetector(epStateMap);
//            Gossiper.instance.applyStateLocally(epStateMap);
        	start = System.currentTimeMillis();
            Gossiper.notifyFailureDetectorStatic(stub, stub.getEndpointStateMap(), epStateMap, stub.getFailureDetector());
            end = System.currentTimeMillis();
            notifyFD = end - start;
        	start = System.currentTimeMillis();
            int[] result = Gossiper.applyStateLocallyStatic(stub, epStateMap);
            long mockExecTime = message.getWakeUpTime() - System.currentTimeMillis();
            if (mockExecTime >= 0) {
                try {
                    Thread.sleep(mockExecTime);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else if (mockExecTime < -10) {
                logger.debug(to + " executing past message " + mockExecTime);
            }
            end = System.currentTimeMillis();
            applyState = end - start;
            newNode = result[0];
            newNodeToken = result[1];
            newRestart = result[2];
            newVersion = result[3];
            newVersionToken = result[4];
            bootstrapCount = result[5];
            normalCount = result[6];
        }

        Gossiper.instance.checkSeedContact(from);

        start = System.currentTimeMillis();
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
        end = System.currentTimeMillis();
        long examine = end - start;

        MessageIn<GossipDigestAck2> gDigestAck2Message = MessageIn.create(to,  new GossipDigestAck2(deltaEpStateMap), emptyMap, 
                MessagingService.Verb.GOSSIP_DIGEST_ACK2, MessagingService.VERSION_12);
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
        long sleepTime = WholeClusterSimulator.bootGossipExecRecords[bootNodeNum] + 
                WholeClusterSimulator.normalGossipExecRecords[normalNodeNum];
//        System.out.println("should sleep " + sleepTime);
        long wakeUpTime = System.currentTimeMillis() + sleepTime;
        gDigestAck2Message.setWakeUpTime(wakeUpTime);
        gDigestAck2Message.setTo(from);
        long sendTime = System.currentTimeMillis();
        for (InetAddress observedNode : deltaEpStateMap.keySet()) {
            int version = Gossiper.getMaxEndpointStateVersion(deltaEpStateMap.get(observedNode));
            logger.info("propagate info of " + observedNode + " to " + from + " version " + version);
            logger.info(to + " Receive ack:" + receiveTime + " (" + (notifyFD + applyState + examine) + "ms)" + " ; Send ack2:" + sendTime + 
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
                    " ; Forwarding " + observedNode + " to " + from + " version " + version);
        }
        for (InetAddress address : newerVersion.keySet()) {
            logger.info(to + " Receive ack:" + receiveTime + " (" + (notifyFD + applyState + examine) + "ms)" + " ; Send ack2:" + sendTime + 
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
            
        }
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        WholeClusterSimulator.ackQueue.add(gDigestAck2Message);
    }
}
