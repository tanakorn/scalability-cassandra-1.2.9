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
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator;

public class SimulatedGossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAck2VerbHandler.class);

    @SuppressWarnings("unchecked")
    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
    	InetAddress from = message.from;
        InetAddress to = message.to;
        GossiperStub receiverStub = WholeClusterSimulator.stubGroup.getStub(to);
        GossiperStub senderStub = WholeClusterSimulator.stubGroup.getStub(from);
        if (logger.isTraceEnabled())
        {
            logger.trace("Received a GossipDigestAck2Message from {}", from);
        }
//        if (!Gossiper.instance.isEnabled())
//        {
//            if (logger.isTraceEnabled())
//                logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
//            return;
//        }

        GossipDigestAck2 gDigestAck2Message = message.payload;
        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
//        int epStateMapSize = remoteEpStateMap.size();
//        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
//        for (InetAddress observedNode : WholeClusterSimulator.observedNodes) {
//            if (remoteEpStateMap.keySet().contains(observedNode)) {
//                EndpointState localEpState = stub.getEndpointStateMap().get(observedNode);
//                EndpointState remoteEpState = remoteEpStateMap.get(observedNode);
//                int remoteGen = remoteEpState.getHeartBeatState().getGeneration();
//                int remoteVersion = Gossiper.getMaxEndpointStateVersion(remoteEpState);
//                boolean newer = false;
//                if (localEpState == null) {
//                    newer = true;
//                } else {
//                    synchronized (localEpState) {
//                        int localGen = localEpState.getHeartBeatState().getGeneration();
//                        if (localGen < remoteGen) {
//                            newer = true;
//                        } else if (localGen == remoteGen) {
//                            int localVersion = Gossiper.getMaxEndpointStateVersion(localEpState);
//                            if (localVersion < remoteVersion) {
//                                newer = true;
//                            }
//                        }
//                    }
//                }
//                if (newer) {
//                    double hbAverage = 0;
//                    FailureDetector fd = (FailureDetector) stub.failureDetector;
//                    if (fd.arrivalSamples.containsKey(observedNode)) {
//                        hbAverage = fd.arrivalSamples.get(observedNode).mean();
//                    }
//                    logger.info(to + " receive info of " + observedNode + " from " + from + 
//                            " generation " + remoteGen + " version " + remoteVersion + " gossip_average " + hbAverage);
//                    newerVersion.put(observedNode, remoteVersion);
//                }
//            }
//        }
        /* Notify the Failure Detector */
//        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
//        Gossiper.instance.applyStateLocally(remoteEpStateMap);
        Map<InetAddress, double[]> updatedNodeInfo = Gossiper.notifyFailureDetectorStatic(receiverStub, receiverStub.getEndpointStateMap(), remoteEpStateMap, receiverStub.getFailureDetector());
        Object[] result = Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
        long mockExecTime = message.getWakeUpTime() - System.currentTimeMillis();
        if (mockExecTime >= 0) {
            try {
//                Thread.sleep(mockExecTime);
                Thread.sleep(message.getSleepTime());
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else if (mockExecTime < -10) {
            logger.debug(to + " executing past message " + mockExecTime);
        }
        int bootstrapCount = (int) result[5];
        int normalCount = (int) result[6];
        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
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
        String syncId = from + "_" + message.payload.syncId;
        long syncReceivedTime = receiverStub.syncReceivedTime.get(syncId);
        receiverStub.syncReceivedTime.remove(syncId);
        long tmpCurrent = System.currentTimeMillis();
        long ack2HandlerTime = tmpCurrent - receiveTime;
        long allHandlerTime = tmpCurrent - syncReceivedTime;
        String ackId = from + "_" + message.payload.ackId;
        int sendingBoot = receiverStub.ackNewVersionBoot.get(ackId);
        receiverStub.ackNewVersionBoot.remove(ackId);
        int sendingNormal = receiverStub.ackNewVersionNormal.get(ackId);
        receiverStub.ackNewVersionNormal.remove(ackId);
        int allBoot = sendingBoot + bootstrapCount;
        int allNormal = sendingNormal + normalCount;
        if (allBoot != 0 || allNormal != 0) {
            logger.info(to + " apply gossip_all boot " + allBoot + " normal " + allNormal);
            logger.info(to + " executes gossip_all took " + allHandlerTime + " ms");
        }
        if (bootstrapCount != 0 || normalCount != 0) {
            logger.info(to + " apply gossip_ack2 boot " + bootstrapCount + " normal " + normalCount);
            logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms");
        }
    }
}
