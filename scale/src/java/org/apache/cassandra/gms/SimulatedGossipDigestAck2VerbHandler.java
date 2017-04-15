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
        Object[] result = Gossiper.applyStateLocallyStatic(receiverStub, remoteEpStateMap);
        int bootstrapCount = (int) result[5];
        int normalCount = (int) result[6];
        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
        int realUpdate = (int) result[9];
        updatedNodeInfo = (Map<InetAddress, double[]>) result[8];
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
        if (bootstrapCount != 0 || normalCount != 0) {
            logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms ; apply boot " + bootstrapCount 
                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
                    + receiverCurrentVersion + " ; transmission " + transmissionTime);
        }
    }
}
