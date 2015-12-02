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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

import edu.uchicago.cs.ucare.util.Klogger;

public class GossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAck2VerbHandler.class);
    private Set<InetAddress> seenAddresses = new HashSet<InetAddress>();

    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
    	long start, end; 
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
        {
            logger.trace("Received a GossipDigestAck2Message from {}", from);
        }
        if (!Gossiper.instance.isEnabled())
        {
            if (logger.isTraceEnabled())
                logger.trace("Ignoring GossipDigestAck2Message because gossip is disabled");
            return;
        }

        int ack2Hash = message.payload.hashCode();
        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        int epStateMapSize = remoteEpStateMap.size();
        int before = Gossiper.instance.endpointStateMap.size();
        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
        for (InetAddress observedNode : FailureDetector.observedNodes) {
            if (remoteEpStateMap.keySet().contains(observedNode)) {
                EndpointState localEpState = Gossiper.instance.getEndpointStateForEndpoint(observedNode);
                EndpointState remoteEpState = remoteEpStateMap.get(observedNode);
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
                    Klogger.logger.info("receive info of " + observedNode + " from " + from + 
                            " generation " + remoteGen + " version " + remoteVersion);
                    newerVersion.put(observedNode, remoteVersion);
                }
            }
        }
        
        /* Notify the Failure Detector */
        start = System.currentTimeMillis();
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        end = System.currentTimeMillis();
        long notifyFD = end - start;
        start = System.currentTimeMillis();
        int[] result = Gossiper.instance.applyStateLocally(remoteEpStateMap);
        int newNode = result[0];
        int newNodeToken = result[1];
        int newRestart = result[2];
        int newVersion = result[3];
        int newVersionToken = result[4];
//        Klogger.logger.info("Receive ack2:" + ack2Hash +
//                " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
//                " newVersion=" + newVersion + " newVersionToken=" + newVersionToken);
        for (InetAddress address : newerVersion.keySet()) {
            Klogger.logger.info("Receive ack2:" + ack2Hash + 
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
        }
        end = System.currentTimeMillis();
        long applyState = end - start;
        int after = Gossiper.instance.endpointStateMap.size();
        Klogger.logger.info("Ack2Handler for " + from + " notifyFD took {} ms, applyState took {} ms", notifyFD, applyState);
        Klogger.logger.info("Processing Ack2 receiving = " + epStateMapSize + " ; before = " + before + " ; after = " + after);
        if (!seenAddresses.contains(from)) {
            Klogger.logger.info("see " + from + " " + Gossiper.instance.endpointStateMap.keySet());
            seenAddresses.add(from);
        }
    }
}
