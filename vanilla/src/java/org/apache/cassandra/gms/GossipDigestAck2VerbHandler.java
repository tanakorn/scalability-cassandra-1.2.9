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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

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
        InetAddress to = FBUtilities.getBroadcastAddress();
        Klogger.logger.info(to + " doVerb ack2");
        int currentVersion = StorageService.instance.getTokenMetadata().tokenToEndpointMap.size() / 1024;
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

        long transmissionTime = receiveTime - message.payload.getCreatedTime();
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
        Object[] result = Gossiper.instance.applyStateLocally(remoteEpStateMap);
        end = System.currentTimeMillis();
        long applyState = end - start;
        int newNode = (int) result[0];
        int newNodeToken = (int) result[1];
        int newRestart = (int) result[2];
        int newVersion = (int) result[3];
        int newVersionToken = (int) result[4];
        int bootstrapCount = (int) result[5];
        int normalCount = (int) result[6];
        Set<InetAddress> updatedNodes = (Set<InetAddress>) result[7];
        int realUpdate = (int) result[8];
        for (InetAddress receivingAddress : updatedNodes) {
            EndpointState ep = Gossiper.instance.getEndpointStateForEndpoint(receivingAddress);
            Klogger.logger.info(to + " is hop " + ep.hopNum + " for " + receivingAddress + " with version " + ep.getHeartBeatState().getHeartBeatVersion() + " from " + from);
        }
        String syncId = from + "_" + message.payload.syncId;
        long syncReceivedTime = Gossiper.instance.syncReceivedTime.get(syncId);
        Gossiper.instance.syncReceivedTime.remove(syncId);
        long tmpCurrent = System.currentTimeMillis();
        long ack2HandlerTime = tmpCurrent - receiveTime;
        long allHandlerTime = tmpCurrent - syncReceivedTime;
        String ackId = from + "_" + message.payload.ackId;
        int sendingBoot = Gossiper.instance.ackNewVersionBoot.get(ackId);
        Gossiper.instance.ackNewVersionBoot.remove(ackId);
        int sendingNormal = Gossiper.instance.ackNewVersionNormal.get(ackId);
        Gossiper.instance.ackNewVersionNormal.remove(ackId);
        int allBoot = sendingBoot + bootstrapCount;
        int allNormal = sendingNormal + normalCount;
        if (allBoot != 0 || allNormal != 0) {
            Klogger.logger.info(to + " executes gossip_all took " + allHandlerTime + " ms ; apply boot " + allBoot + " normal " + allNormal);
        }
        if (bootstrapCount != 0 || normalCount != 0) {
            Klogger.logger.info(to + " executes gossip_ack2 took " + ack2HandlerTime + " ms ; apply boot " + bootstrapCount 
                    + " normal " + normalCount + " realUpdate " + realUpdate + " currentVersion " 
                    + currentVersion + " ; transmission " + transmissionTime);
        }
        Klogger.logger.info("Ack2Handler for " + from + " notifyFD took {} ms, applyState took {} ms", notifyFD, applyState);
    }
}
