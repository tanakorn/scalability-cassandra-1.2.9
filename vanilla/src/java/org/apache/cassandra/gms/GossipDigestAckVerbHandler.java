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
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import edu.uchicago.cs.ucare.util.Klogger;

public class GossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAckVerbHandler.class);

    @SuppressWarnings("unchecked")
    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
        InetAddress from = message.from;
        InetAddress to = FBUtilities.getBroadcastAddress();
        Klogger.logger.info(to + " doVerb ack");
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
            return;
        }

        int currentVersion = StorageService.instance.getTokenMetadata().tokenToEndpointMap.size() / 1024;
        GossipDigestAck gDigestAckMessage = message.payload;
        long transmissionTime = receiveTime - gDigestAckMessage.getCreatedTime();
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        
        int bootstrapCount = 0;
        int normalCount = 0;
        int realUpdate = 0;
        if ( epStateMap.size() > 0 )
        {
            /* Notify the Failure Detector */
            Gossiper.instance.notifyFailureDetector(epStateMap);
            Object[] result = Gossiper.instance.applyStateLocally(epStateMap);
            bootstrapCount = (int) result[5];
            normalCount = (int) result[6];
            Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
            realUpdate = (int) result[8];
            for (InetAddress receivingAddress : updatedNodes) {
                EndpointState ep = Gossiper.instance.endpointStateMap.get(receivingAddress);
                Klogger.logger.info(to + " is hop " + ep.hopNum + " for " + receivingAddress + " with version " + ep.getHeartBeatState().getHeartBeatVersion() + " from " + from);
            }
        }

        Gossiper.instance.checkSeedContact(from);

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
        Map<InetAddress, EndpointState> localEpStateMap = Gossiper.instance.endpointStateMap;
        for (InetAddress sendingAddress : deltaEpStateMap.keySet()) {
            deltaEpStateMap.get(sendingAddress).setHopNum(localEpStateMap.get(sendingAddress).hopNum);
        }
        
        int sendingBoot = 0;
        int sendingNormal = 0;
        for (InetAddress sendingAddress : deltaEpStateMap.keySet()) {
            EndpointState ep = deltaEpStateMap.get(sendingAddress);
            ep.setHopNum(localEpStateMap.get(sendingAddress).hopNum);
            VersionedValue val = ep.applicationState.get(ApplicationState.STATUS);
            if (val != null) {
                if (val.value.indexOf(VersionedValue.STATUS_BOOTSTRAPPING) == 0) {
                    sendingBoot++;
//                    Klogger.logger.info("ack " + to + " sending boot of " + sendingAddress + " to " + from + " version " + val.version);
                } else if (val.value.indexOf(VersionedValue.STATUS_NORMAL) == 0) {
                    sendingNormal++;
//                    Klogger.logger.info("ack " + to + " sending normal of " + sendingAddress + " to " + from + " version " + val.version);
                }
            }
        }
        if (sendingBoot != 0 || sendingNormal != 0) {
            
        }

        MessageOut<GossipDigestAck2> gDigestAck2Message = 
                new MessageOut<GossipDigestAck2>(MessagingService.Verb.GOSSIP_DIGEST_ACK2,
                     new GossipDigestAck2(deltaEpStateMap, message.payload.syncId, message.payload.msgId),
                     GossipDigestAck2.serializer);
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        gDigestAck2Message.payload.setCreatedTime(System.currentTimeMillis());
        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        long ackHandlerTime = System.currentTimeMillis() - receiveTime;
        if (bootstrapCount != 0 || normalCount != 0) {
            Klogger.logger.info(to + " executes gossip_ack took " + ackHandlerTime + " ms ; apply boot " + bootstrapCount 
                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
                    + currentVersion + " ; transmission " + transmissionTime);
        }
    }
}
