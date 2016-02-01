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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator;

public class SimulatedGossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAckVerbHandler.class);
    private static final Map<String, byte[]> emptyMap = Collections.<String, byte[]>emptyMap();
    
    @SuppressWarnings("unchecked")
    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
        InetAddress from = message.from;
        InetAddress to = message.to;
        logger.info(to + " doVerb ack");
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
//        if (!Gossiper.instance.isEnabled())
//        {
//            if (logger.isTraceEnabled())
//                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
//            return;
//        }

        GossipDigestAck gDigestAckMessage = message.payload;
        long transmissionTime = receiveTime - gDigestAckMessage.getCreatedTime();
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        
        GossiperStub receiverStub = WholeClusterSimulator.stubGroup.getStub(to);
        GossiperStub senderStub = WholeClusterSimulator.stubGroup.getStub(from);
        
        int senderCurrentVersion = senderStub.getTokenMetadata().endpointWithTokens.size();
        
        int bootstrapCount = 0;
        int normalCount = 0;
        int realUpdate = 0;
        int receiverCurrentVersion = receiverStub.getTokenMetadata().endpointWithTokens.size();

        Map<InetAddress, double[]> updatedNodeInfo = null;
        Object[] result = null;
        if ( epStateMap.size() > 0 )
        {
            /* Notify the Failure Detector */
//            Gossiper.instance.notifyFailureDetector(epStateMap);
//            Gossiper.instance.applyStateLocally(epStateMap);
        	updatedNodeInfo = Gossiper.notifyFailureDetectorStatic(receiverStub, receiverStub.getEndpointStateMap(), 
        	        epStateMap, receiverStub.getFailureDetector());
            result = Gossiper.applyStateLocallyStatic(receiverStub, epStateMap);
            try {
                Thread.sleep(message.getSleepTime());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
//            long mockExecTime = message.getWakeUpTime() - System.currentTimeMillis();
//            if (mockExecTime >= 0) {
//                try {
////                    Thread.sleep(mockExecTime);
//                    Thread.sleep(message.getSleepTime());
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            } else if (mockExecTime < -10) {
//                logger.debug(to + " executing past message " + mockExecTime);
//            }
            
        }

        Gossiper.instance.checkSeedContact(from);

        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        Map<InetAddress, EndpointState> deltaEpStateMap = new HashMap<InetAddress, EndpointState>();
        for( GossipDigest gDigest : gDigestList )
        {
            InetAddress addr = gDigest.getEndpoint();
//            EndpointState localEpStatePtr = Gossiper.instance.getStateForVersionBiggerThan(addr, gDigest.getMaxVersion());
            EndpointState localEpStatePtr = Gossiper.getStateForVersionBiggerThanStatic(receiverStub.getEndpointStateMap(),
            		addr, gDigest.getMaxVersion());
            if ( localEpStatePtr != null )
                deltaEpStateMap.put(addr, localEpStatePtr.copy());
        }

        Map<InetAddress, EndpointState> localEpStateMap = receiverStub.getEndpointStateMap();
        for (InetAddress sendingAddress : deltaEpStateMap.keySet()) {
            deltaEpStateMap.get(sendingAddress).setHopNum(localEpStateMap.get(sendingAddress).hopNum);
        }
        MessageIn<GossipDigestAck2> gDigestAck2Message = 
                MessageIn.create(to,  new GossipDigestAck2(deltaEpStateMap, message.payload.syncId, message.payload.msgId), 
                        emptyMap, MessagingService.Verb.GOSSIP_DIGEST_ACK2, MessagingService.VERSION_12);
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
                        if (!senderStub.getTokenMetadata().endpointWithTokens.contains(address)) {
                            normalNodeNum++;
                        }
                    }
                }
            }
        }
        
        int roundCurrentVersion = (senderCurrentVersion / 8) * 8 + 1;
        int roundNormalVersion = (normalNodeNum / 4) * 4 + 1;

        long sleepTime = WholeClusterSimulator.bootGossipExecRecords[bootNodeNum];
        if (normalNodeNum != 0) {
            sleepTime += WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, roundNormalVersion);
        }
        long wakeUpTime = System.currentTimeMillis() + sleepTime;
        gDigestAck2Message.setWakeUpTime(wakeUpTime);
        gDigestAck2Message.setSleepTime(sleepTime);
        gDigestAck2Message.setTo(from);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        gDigestAck2Message.payload.setCreatedTime(System.currentTimeMillis());
        WholeClusterSimulator.msgQueues.get(from).add(gDigestAck2Message);
//        WholeClusterSimulator.msgQueue.add(gDigestAck2Message);
        long ackHandlerTime = System.currentTimeMillis() - receiveTime;
        if (result != null) {
            bootstrapCount = (int) result[5];
            normalCount = (int) result[6];
            Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
            realUpdate = (int) result[9];
            if (!updatedNodes.isEmpty()) {
                StringBuilder sb = new StringBuilder(to.toString());
                sb.append(" hop ");
                for (InetAddress receivingAddress : updatedNodes) {
                    EndpointState ep = receiverStub.getEndpointStateMap().get(receivingAddress);
                    sb.append(ep.hopNum);
                    sb.append(",");
                }
                logger.info(sb.toString());
            }
            if (updatedNodeInfo != null && !updatedNodeInfo.isEmpty()) {
                StringBuilder sb = new StringBuilder(to.toString());
                sb.append(" t_silence ");
                for (InetAddress address : updatedNodeInfo.keySet()) {
                    double[] updatedInfo = updatedNodeInfo.get(address); 
                    sb.append(updatedInfo[0]);
                    sb.append(":");
                    sb.append(updatedInfo[1]);
                    sb.append(",");
                }
                logger.info(sb.toString());
            }
            updatedNodeInfo = (Map<InetAddress, double[]>) result[8];
            if (!updatedNodeInfo.isEmpty()) {
                StringBuilder sb = new StringBuilder(to.toString());
                sb.append(" t_silence ");
                for (InetAddress address : updatedNodeInfo.keySet()) {
                    double[] updatedInfo = updatedNodeInfo.get(address); 
                    sb.append(updatedInfo[0]);
                    sb.append(":");
                    sb.append(updatedInfo[1]);
                    sb.append(",");
                }
                logger.info(sb.toString());
            }
//            if (bootstrapCount != 0 || normalCount != 0) {
//                logger.info(to + " executes gossip_ack took " + ackHandlerTime + " ms ; apply boot " + bootstrapCount 
//                        + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
//                        + receiverCurrentVersion + " ; transmission " + transmissionTime);
//            }
            logger.info(to + " executes gossip_ack took " + ackHandlerTime + " ms ; apply boot " + bootstrapCount 
                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
                    + receiverCurrentVersion + " ; transmission " + transmissionTime);
        }
        long execTime = System.currentTimeMillis() - receiveTime;
        logger.info("doVerb end ack " + execTime);
    }
}
