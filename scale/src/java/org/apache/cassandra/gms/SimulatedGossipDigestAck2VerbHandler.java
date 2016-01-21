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

//import edu.uchicago.cs.ucare.cassandra.gms.GossipProcessingMetric;
import edu.uchicago.cs.ucare.cassandra.gms.GossiperStub;
import edu.uchicago.cs.ucare.cassandra.gms.RandomGossipProcessingMetric;

public class SimulatedGossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    private static final Logger logger = LoggerFactory.getLogger(SimulatedGossipDigestAck2VerbHandler.class);

    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
    	InetAddress from = message.from;
        InetAddress to = message.to;
        GossiperStub stub = RandomGossipProcessingMetric.stubGroup.getStub(to);
        int numBefore = stub.getTokenMetadata().tokenToEndpointMap.size();
        long receiveTime = System.currentTimeMillis();
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

        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        
        /* Notify the Failure Detector */
//        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
//        Gossiper.instance.applyStateLocally(remoteEpStateMap);
        Gossiper.notifyFailureDetectorStatic(stub.getEndpointStateMap(), remoteEpStateMap);
        Object[] result = Gossiper.applyStateLocallyStatic(stub, remoteEpStateMap);
        long tmpCurrent = System.currentTimeMillis();
        long ack2HandlerTime = tmpCurrent - receiveTime;
        int bootstrapCount = (int) result[5];
        int normalCount = (int) result[6];
        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
        long copyTime = (long) result[8];
        long updateTime = (long) result[9];
        int realNormalUpdate = (int) result[10];
        int numAfter = stub.getTokenMetadata().tokenToEndpointMap.size();
        
        if (bootstrapCount != 0 || normalCount != 0) {
            logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms ; apply boot " 
                    + bootstrapCount + " normal " + normalCount + " realNormalUpdate " + realNormalUpdate 
                    + " ; transmission n/a ; before " + numBefore + " after " + numAfter
                    + " ; copytime " + copyTime + " updatetime " + updateTime);
        }

    }
}
