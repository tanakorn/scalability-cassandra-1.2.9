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

import edu.uchicago.cs.ucare.cassandra.gms.TimeManager;
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
        long receiveTime = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp();
    	InetAddress from = message.from;
        InetAddress to = message.to;
//        logger.info(to + " doVerb ack2");
        GossiperStub receiverStub = WholeClusterSimulator.stubGroup.getStub(to);
        GossiperStub senderStub = WholeClusterSimulator.stubGroup.getStub(from);
        int receiverCurrentVersion = receiverStub.getTokenMetadata().endpointWithTokens.size();
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
        long transmissionTime = receiveTime - message.createdTime;
        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        /* Notify the Failure Detector */
//        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
//        Gossiper.instance.applyStateLocally(remoteEpStateMap);
        Map<InetAddress, double[]> updatedNodeInfo = Gossiper.notifyFailureDetectorStatic(receiverStub, receiverStub.getEndpointStateMap(), remoteEpStateMap, receiverStub.getFailureDetector());
//        Object[] result = Gossiper.determineApplyStateLocallyStatic(receiverStub, remoteEpStateMap);
//        int tmpNormalCount = (int) result[6];
//        try {
//            int realUpdate = (int) result[9];
//            int roundCurrentVersion = (int) (Math.round(receiverCurrentVersion / 8.0) * 8 + 1);
////            int roundNormalVersion = (int) (Math.round(realUpdate / 4.0) * 4 + 1);
//            long sleepTime = 0;
//            if (realUpdate != 0) {
//                int floorNormalVersion = (realUpdate / 4) * 4;
//                int ceilingNormalVersion = (realUpdate / 4 + 1) * 4;
//                long floorSleepTime = floorNormalVersion == 0 ? 0 : WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, floorNormalVersion);
//                long ceilingSleepTime = WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, ceilingNormalVersion);
//                sleepTime = (floorSleepTime + ceilingSleepTime) / 2;
//                long realSleep = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp();
//                if (sleepTime > 0) {
//                    Thread.sleep(sleepTime);
//                }
//                realSleep = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp() - realSleep;
//                long lateness = realSleep - sleepTime;
//                lateness = lateness < 0 ? 0 : lateness;
//                WholeClusterSimulator.totalRealSleep += realSleep;
//                WholeClusterSimulator.totalExpectedSleep += sleepTime;
//                WholeClusterSimulator.totalProcLateness += lateness;
//                WholeClusterSimulator.numProc++;
//                WholeClusterSimulator.procLatenessList.add(lateness);
//                if (sleepTime != 0) {
//                    WholeClusterSimulator.percentProcLatenessList.add(((((double) realSleep) / (double) sleepTime) - 1) * 100);
//                } else {
//                    WholeClusterSimulator.percentProcLatenessList.add(0.0);
//                }
//                if (lateness > WholeClusterSimulator.maxProcLateness) {
//                    WholeClusterSimulator.maxProcLateness = lateness;
//                }
//            }
//            Thread.sleep(message.getSleepTime());
//            long sleepTime = realUpdate == 0 ? 0 : WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, roundNormalVersion);
//            long realSleep = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp();
//            Thread.sleep(sleepTime);
//            realSleep = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp() - realSleep;
//            long lateness = realSleep - sleepTime;
//            lateness = lateness < 0 ? 0 : lateness;
//            logger.info("Processing lateness " + lateness);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        Object[] result2 = Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
        Object[] result = Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
//        for (int i = 0; i < result.length; ++i) {
//            if (!result[i].equals(result2[i])) {
//                System.out.println(i + " index is not the same");
//            }
//        }
//        long mockExecTime = message.getWakeUpTime() - TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp();
//        if (mockExecTime >= 0) {
//            try {
////                Thread.sleep(mockExecTime);
//                Thread.sleep(message.getSleepTime());
//            } catch (InterruptedException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        } else if (mockExecTime < -10) {
//            logger.debug(to + " executing past message " + mockExecTime);
//        }
        int bootstrapCount = (int) result[5];
        int normalCount = (int) result[6];
        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
        int realUpdate = (int) result[9];
//        if (!updatedNodes.isEmpty()) {
//            StringBuilder sb = new StringBuilder(to.toString());
//            sb.append(" hop ");
//            for (InetAddress receivingAddress : updatedNodes) {
//                EndpointState ep = receiverStub.getEndpointStateMap().get(receivingAddress);
//                sb.append(ep.hopNum);
//                sb.append(",");
//            }
//            logger.info(sb.toString());
//        }
//        if (!updatedNodeInfo.isEmpty()) {
//            StringBuilder sb = new StringBuilder(to.toString());
//            sb.append(" t_silence ");
//            for (InetAddress address : updatedNodeInfo.keySet()) {
//                double[] updatedInfo = updatedNodeInfo.get(address); 
//                sb.append(updatedInfo[0]);
//                sb.append(":");
//                sb.append(updatedInfo[1]);
//                sb.append(",");
//            }
//            logger.info(sb.toString());
//        }
        updatedNodeInfo = (Map<InetAddress, double[]>) result[8];
//        if (!updatedNodeInfo.isEmpty()) {
//            StringBuilder sb = new StringBuilder(to.toString());
//            sb.append(" t_silence ");
//            for (InetAddress address : updatedNodeInfo.keySet()) {
//                double[] updatedInfo = updatedNodeInfo.get(address); 
//                sb.append(updatedInfo[0]);
//                sb.append(":");
//                sb.append(updatedInfo[1]);
//                sb.append(",");
//            }
//            logger.info(sb.toString());
//        }
        String syncId = from + "_" + message.payload.syncId;
        Long syncReceivedTime = receiverStub.syncReceivedTime.get(syncId);
        receiverStub.syncReceivedTime.remove(syncId);
        long tmpCurrent = TimeManager.instance.getCurrentTimeMillisFromBaseTimeStamp();
        long ack2HandlerTime = tmpCurrent - receiveTime;
        long allHandlerTime = tmpCurrent - (syncReceivedTime == null? 0 : syncReceivedTime);
        String ackId = from + "_" + message.payload.ackId;
        Integer sendingBoot = receiverStub.ackNewVersionBoot.get(ackId);
        receiverStub.ackNewVersionBoot.remove(ackId);
        Integer sendingNormal = receiverStub.ackNewVersionNormal.get(ackId);
        receiverStub.ackNewVersionNormal.remove(ackId);
        int allBoot = (sendingBoot == null? 0 : sendingBoot) + bootstrapCount;
        int allNormal = (sendingNormal == null? 0 : sendingNormal) + normalCount;
//        if (allBoot != 0 || allNormal != 0) {
//            logger.info(to + " executes gossip_all took " + allHandlerTime + " ms ; apply boot " + allBoot + " normal " + allNormal);
//        }
//        if (bootstrapCount != 0 || normalCount != 0) {
//            logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms ; apply boot " + bootstrapCount 
//                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
//                    + receiverCurrentVersion + " ; transmission " + transmissionTime);
//        }
    }
}
