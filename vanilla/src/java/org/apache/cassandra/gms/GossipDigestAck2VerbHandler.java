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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

import edu.uchicago.cs.ucare.util.Klogger;

public class GossipDigestAck2VerbHandler implements IVerbHandler<GossipDigestAck2>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipDigestAck2VerbHandler.class);
//    private Set<InetAddress> seenAddresses = new HashSet<InetAddress>();

    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
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

        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
        for (InetAddress observedNode : remoteEpStateMap.keySet()) {
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
//                double hbAverage = 0;
//                FailureDetector fd = (FailureDetector) FailureDetector.instance;
//                if (fd.arrivalSamples.containsKey(observedNode)) {
//                    hbAverage = fd.arrivalSamples.get(observedNode).mean();
//                }
//                Klogger.logger.info("receive info of " + observedNode + " from " + from + 
//                        " generation " + remoteGen + " version " + remoteVersion + " gossip_average " + hbAverage);
                newerVersion.put(observedNode, remoteVersion);
            }
        }
        
        /* Notify the Failure Detector */
        start = System.currentTimeMillis();
        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
        end = System.currentTimeMillis();
        long notifyFD = end - start;
        start = System.currentTimeMillis();
        int[] result = Gossiper.instance.applyStateLocally(remoteEpStateMap);
        end = System.currentTimeMillis();
        long applyState = end - start;
        int newNode = result[0];
        int newNodeToken = result[1];
        int newRestart = result[2];
        int newVersion = result[3];
        int newVersionToken = result[4];
        int bootstrapCount = result[5];
        int normalCount = result[6];
        for (InetAddress address : newerVersion.keySet()) {
            Klogger.logger.info("Receive ack2:" + receiveTime + " (" + (notifyFD + applyState) + "ms)" +
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
        }
        Klogger.logger.info("Ack2Handler for " + from + " notifyFD took {} ms, applyState took {} ms", notifyFD, applyState);
    }
}
