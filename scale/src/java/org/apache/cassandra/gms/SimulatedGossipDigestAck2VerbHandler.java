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
//    private Set<InetAddress> seenAddresses = new HashSet<InetAddress>();

    public void doVerb(MessageIn<GossipDigestAck2> message, String id)
    {
        long receiveTime = System.currentTimeMillis();
    	long start, end; 
    	InetAddress from = message.from;
        InetAddress to = message.to;
        GossiperStub stub = WholeClusterSimulator.stubGroup.getStub(to);
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

        int ack2Hash = message.payload.hashCode();
        Map<InetAddress, EndpointState> remoteEpStateMap = message.payload.getEndpointStateMap();
        int epStateMapSize = remoteEpStateMap.size();
        Map<InetAddress, Integer> newerVersion = new HashMap<InetAddress, Integer>();
        for (InetAddress observedNode : WholeClusterSimulator.observedNodes) {
            if (remoteEpStateMap.keySet().contains(observedNode)) {
                EndpointState localEpState = stub.getEndpointStateMap().get(observedNode);
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
                    double hbAverage = 0;
                    FailureDetector fd = (FailureDetector) stub.failureDetector;
                    if (fd.arrivalSamples.containsKey(observedNode)) {
                        hbAverage = fd.arrivalSamples.get(observedNode).mean();
                    }
                    logger.info(to + " receive info of " + observedNode + " from " + from + 
                            " generation " + remoteGen + " version " + remoteVersion + " gossip_average " + hbAverage);
                    newerVersion.put(observedNode, remoteVersion);
                }
            }
        }
        /* Notify the Failure Detector */
//        Gossiper.instance.notifyFailureDetector(remoteEpStateMap);
//        Gossiper.instance.applyStateLocally(remoteEpStateMap);
        start = System.currentTimeMillis();
        Gossiper.notifyFailureDetectorStatic(stub, stub.getEndpointStateMap(), remoteEpStateMap, stub.getFailureDetector());
        end = System.currentTimeMillis();
        long notifyFD = end - start;
        start = System.currentTimeMillis();
        int[] result = Gossiper.applyStateLocallyStatic(stub, remoteEpStateMap);
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
        end = System.currentTimeMillis();
        long sleeptime = message.getSleepTime();
        long applyState = end - start;
        int newNode = result[0];
        int newNodeToken = result[1];
        int newRestart = result[2];
        int newVersion = result[3];
        int newVersionToken = result[4];
        int bootstrapCount = result[5];
        int normalCount = result[6];
        for (InetAddress address : newerVersion.keySet()) {
            logger.info(to + " Receive ack2:" + receiveTime + " (" + (notifyFD + applyState) + "ms)" +
                    " ; newNode=" + newNode + " newNodeToken=" + newNodeToken + " newRestart=" + newRestart + 
                    " newVersion=" + newVersion + " newVersionToken=" + newVersionToken +
                    " bootstrapCount=" + bootstrapCount + " normalCount=" + normalCount +
                    " ; Absorbing " + address + " from " + from + " version " + newerVersion.get(address));
        }
//        int after = Gossiper.instance.endpointStateMap.size();
//        logger.info("Ack2Handler for " + from + " notifyFD took {} ms, applyState took {} ms", notifyFD, applyState);
//        logger.info("Processing Ack2 receiving = " + epStateMapSize + " ; before = " + before + " ; after = " + after);
//        if (!seenAddresses.contains(from)) {
//            logger.info("see " + from + " " + Gossiper.instance.endpointStateMap.keySet());
//            seenAddresses.add(from);
//        }
    }
}
