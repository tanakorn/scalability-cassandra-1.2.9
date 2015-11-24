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
        List<GossipDigest> gDigestList = gDigestAckMessage.getGossipDigestList();
        Map<InetAddress, EndpointState> epStateMap = gDigestAckMessage.getEndpointStateMap();
        
        GossiperStub stub = WholeClusterSimulator.stubGroup.getStub(to);

        if ( epStateMap.size() > 0 )
        {
            /* Notify the Failure Detector */
//            Gossiper.instance.notifyFailureDetector(epStateMap);
//            Gossiper.instance.applyStateLocally(epStateMap);
            Gossiper.notifyFailureDetectorStatic(stub, stub.getEndpointStateMap(), epStateMap, stub.getFailureDetector());
            Gossiper.applyStateLocallyStatic(stub, epStateMap);

        }
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
        if (logger.isTraceEnabled())
            logger.trace("Sending a GossipDigestAck2Message to {}", from);
        WholeClusterSimulator.ackQueue.add(gDigestAck2Message);
    }
}
