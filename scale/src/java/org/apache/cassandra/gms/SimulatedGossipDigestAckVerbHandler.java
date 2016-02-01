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
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;


//import edu.uchicago.cs.ucare.cassandra.gms.GossipProcessingMetric;
import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.RandomGossipProcessingMetric;

public class SimulatedGossipDigestAckVerbHandler implements IVerbHandler<GossipDigestAck>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAckVerbHandler.class);
    
    public void doVerb(MessageIn<GossipDigestAck> message, String id)
    {
        InetAddress to = message.to;
        GossiperStub stub = RandomGossipProcessingMetric.stubGroup.getStub(to);
        int numBefore = stub.getTokenMetadata().tokenToEndpointMap.size();
        long receiveTime = System.currentTimeMillis();
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipDigestAckMessage from {}", from);
//        if (!Gossiper.instance.isEnabled())
//        {
//            if (logger.isTraceEnabled())
//                logger.trace("Ignoring GossipDigestAckMessage because gossip is disabled");
//            return;
//        }

        GossipDigestAck gDigestAckMessage = message.payload;
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        

        int bootstrapCount = 0;
        int normalCount = 0;
        long copyTime = 0;
        long updateTime = 0;
        int realNormalUpdate = 0;
        if ( epStateMap.size() > 0 )
        {
            /* Notify the Failure Detector */
//            Gossiper.instance.notifyFailureDetector(epStateMap);
//            Gossiper.instance.applyStateLocally(epStateMap);
            Gossiper.notifyFailureDetectorStatic(stub.getEndpointStateMap(), epStateMap);
            Object[] result = Gossiper.applyStateLocallyStatic(stub, epStateMap);
            bootstrapCount = (int) result[5];
            normalCount = (int) result[6];
            Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
            copyTime = (long) result[8];
            updateTime = (long) result[9];
            realNormalUpdate = (int) result[10];
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
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
//        if (WorstCaseGossiperStub.addressSet.contains(from)) {
//        	MessageIn<GossipDigestAck2> msgIn = WorstCaseGossiperStub.convertOutToIn(gDigestAck2Message);
//        	msgIn.setTo(from);
//            MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_ACK2).doVerb(msgIn, 
//            		Integer.toString(WorstCaseGossiperStub.idGen.incrementAndGet()));
//        } else {
//            MessagingService.instance().sendOneWay(gDigestAck2Message, from);
//        }
        if (RandomGossipProcessingMetric.stubGroup.contains(from)) {
            MessageIn<GossipDigestAck2> msgIn = RandomGossipProcessingMetric.convertOutToIn(gDigestAck2Message);
            msgIn.setTo(from);
            long s = System.currentTimeMillis();
            RandomGossipProcessingMetric.fill();
            MessagingService.instance().getVerbHandler(Verb.GOSSIP_DIGEST_ACK2).doVerb(msgIn, 
                    Integer.toString(RandomGossipProcessingMetric.idGen.incrementAndGet()));
            long t = System.currentTimeMillis() - s;
            logger.info("sc_debug: Doing verb \"" + Verb.GOSSIP_DIGEST_ACK2 + "\" from " + msgIn.from + " took " + t + " ms");
        } else {
            MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        }
//        MessagingService.instance().sendOneWay(gDigestAck2Message, from);
        long ackHandlerTime = System.currentTimeMillis() - receiveTime;
        int numAfter = stub.getTokenMetadata().tokenToEndpointMap.size();
        if (bootstrapCount != 0 || normalCount != 0) {
            logger.info(to + " executes gossip_ack took " + ackHandlerTime + " ms ; apply boot " 
                    + bootstrapCount + " normal " + normalCount + " realNormalUpdate " + realNormalUpdate 
                    + " ; transmission n/a ; before " + numBefore + " after " + numAfter 
                    + " ; copytime " + copyTime + " updatetime " + updateTime);
        }
    }
}
