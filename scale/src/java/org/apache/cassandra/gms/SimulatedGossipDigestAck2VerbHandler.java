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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.WholeClusterSimulator;
import edu.uchicago.cs.ucare.scale.ResumeTask;

public class SimulatedGossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAck2VerbHandler.class);

    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
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
//        Object[] result = Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
        Object[] result = Gossiper.determineApplyStateLocallyStatic(receiverStub, remoteEpStateMap);
//      int tmpNormalCount = (int) result[6];
        int bootstrapCount = (int) result[5];
        int realUpdate = (int) result[9];
        int roundCurrentVersion = (int) (Math.round(receiverCurrentVersion / 8.0) * 8 + 1);
//      int roundNormalVersion = (int) (Math.round(realUpdate / 4.0) * 4 + 1);
        long sleepTime = 0;
        if (realUpdate != 0) {
            int floorNormalVersion = (realUpdate / 4) * 4;
            int ceilingNormalVersion = (realUpdate / 4 + 1) * 4;
            long floorSleepTime = floorNormalVersion == 0 ? 0 : WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, floorNormalVersion);
            long ceilingSleepTime = WholeClusterSimulator.getExecTimeNormal(roundCurrentVersion, ceilingNormalVersion);
            sleepTime = (floorSleepTime + ceilingSleepTime) / 2;
        }
        sleepTime += WholeClusterSimulator.bootGossipExecRecords[bootstrapCount];
        long wakeupTime = System.currentTimeMillis() + sleepTime;
//        WholeClusterSimulator.resumeTimer.schedule(new SimulatedGDA2ResumeTask(wakeupTime, receiveTime, result, message), sleepTime);
//        WholeClusterSimulator.resumeProcessors.schedule(new SimulatedGDA2ResumeTask(wakeupTime, receiveTime, result, message), 
//                sleepTime, TimeUnit.MILLISECONDS);
        if (sleepTime > 0) {
            WholeClusterSimulator.submitResumeTask(new SimulatedGDA2ResumeTask(wakeupTime, sleepTime, receiveTime, result, message, updatedNodeInfo));
        } else {
            new SimulatedGDA2ResumeTask(wakeupTime, sleepTime, receiveTime, result, message, updatedNodeInfo).run();
        }

//        ResumeTask.totalRealSleep += sleepTime;

//        long mockExecTime = message.getWakeUpTime() - System.currentTimeMillis();
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
//        int bootstrapCount = (int) result[5];
//        int normalCount = (int) result[6];
//        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
//        int realUpdate = (int) result[9];
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
//        updatedNodeInfo = (Map<InetAddress, double[]>) result[8];
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
//        String syncId = from + "_" + message.payload.syncId;
//        long syncReceivedTime = receiverStub.syncReceivedTime.get(syncId);
//        receiverStub.syncReceivedTime.remove(syncId);
//        long tmpCurrent = System.currentTimeMillis();
//        long ack2HandlerTime = tmpCurrent - receiveTime;
//        long allHandlerTime = tmpCurrent - syncReceivedTime;
//        String ackId = from + "_" + message.payload.ackId;
//        int sendingBoot = receiverStub.ackNewVersionBoot.get(ackId);
//        receiverStub.ackNewVersionBoot.remove(ackId);
//        int sendingNormal = receiverStub.ackNewVersionNormal.get(ackId);
//        receiverStub.ackNewVersionNormal.remove(ackId);
//        int allBoot = sendingBoot + bootstrapCount;
//        int allNormal = sendingNormal + normalCount;
//        if (allBoot != 0 || allNormal != 0) {
//            logger.info(to + " executes gossip_all took " + allHandlerTime + " ms ; apply boot " + allBoot + " normal " + allNormal);
//        }
//        if (bootstrapCount != 0 || normalCount != 0) {
//            logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms ; apply boot " + bootstrapCount 
//                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
//                    + receiverCurrentVersion + " ; transmission " + transmissionTime);
//        }
    }
    
    public static class SimulatedGDA2ResumeTask extends ResumeTask {
        
        private long receiveTime;
        private Object[] result;
        private MessageIn<GossipDigestAck2> message;
        private Map<InetAddress, double[]> updatedNodeInfo;

        public SimulatedGDA2ResumeTask(long expectedExecutionTime, long sleepTime, 
                long receiveTime, Object[] result, MessageIn<GossipDigestAck2> message, 
                Map<InetAddress, double[]> updatedNodeInfo) {
            super(message.to, expectedExecutionTime, sleepTime);
            this.receiveTime = receiveTime;
            this.result = result;
            this.message = message;
            this.updatedNodeInfo = updatedNodeInfo;
        }

        @Override
        public void resume() {
            long lateness = getLateness();
//            logger.info("Processing lateness " + lateness);
            int bootstrapCount = (int) result[5];
            int normalCount = (int) result[6];
            int realUpdate = (int) result[9];
            InetAddress from = message.from;
            InetAddress to = message.to;
//            logger.info("Processing lateness " + to + " " + System.currentTimeMillis() + " " + expectedExecutionTime + " " + lateness);
            GossiperStub receiverStub = WholeClusterSimulator.stubGroup.getStub(to);
            Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
            Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
            String syncId = from + "_" + message.payload.syncId;
//            long syncReceivedTime = receiverStub.syncReceivedTime.get(syncId);
            receiverStub.syncReceivedTime.remove(syncId);
//            long allHandlerTime = tmpCurrent - syncReceivedTime;
            String ackId = from + "_" + message.payload.ackId;
//            int sendingBoot = receiverStub.ackNewVersionBoot.get(ackId);
            receiverStub.ackNewVersionBoot.remove(ackId);
//            int sendingNormal = receiverStub.ackNewVersionNormal.get(ackId);
            receiverStub.ackNewVersionNormal.remove(ackId);
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
            int receiverCurrentVersion = receiverStub.getTokenMetadata().endpointWithTokens.size();
            GossipDigestAck2 gDigestAck2Message = message.payload;
            long transmissionTime = receiveTime - message.createdTime;
            long ack2HandlerTime = System.currentTimeMillis() - receiveTime;
            if (bootstrapCount != 0 || normalCount != 0) {
                logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms " + sleepTime + " ms ; apply boot " + bootstrapCount 
                        + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
                        + receiverCurrentVersion + " ; transmission " + transmissionTime + " lateness " + lateness);
            }
            long endTime = System.currentTimeMillis();
//            logger.info("Processing finished " + endTime + " " + (endTime - triggeredTime));
            WholeClusterSimulator.isProcessing.get(to).set(false);
        }
        
    }
    
}
