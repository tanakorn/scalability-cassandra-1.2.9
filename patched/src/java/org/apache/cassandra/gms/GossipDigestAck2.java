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

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
public class GossipDigestAck2
{
    public static final IVersionedSerializer<GossipDigestAck2> serializer = new GossipDigestAck2Serializer();

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    final Map<InetAddress, EndpointState> epStateMap;
    final int msgId;
    final int syncId;
    final int ackId;
    long createdTime;

    GossipDigestAck2(Map<InetAddress, EndpointState> epStateMap, int syncId, int ackId)
    {
        this.epStateMap = epStateMap;
        this.msgId = idGenerator.getAndIncrement();
        this.syncId = syncId;
        this.ackId = ackId;
    }

    GossipDigestAck2(Map<InetAddress, EndpointState> epStateMap, int msgId, int syncId, int ackId)
    {
        this.epStateMap = epStateMap;
        this.msgId = msgId;
        this.syncId = syncId;
        this.ackId = ackId;
    }

    public Map<InetAddress, EndpointState> getEndpointStateMap()
    {
         return epStateMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((epStateMap == null) ? 0 : epStateMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GossipDigestAck2 other = (GossipDigestAck2) obj;
        if (epStateMap == null) {
            if (other.epStateMap != null)
                return false;
        } else if (!epStateMap.equals(other.epStateMap))
            return false;
        return true;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }
}

class GossipDigestAck2Serializer implements IVersionedSerializer<GossipDigestAck2>
{
    public void serialize(GossipDigestAck2 ack2, DataOutput dos, int version) throws IOException
    {
        dos.writeInt(ack2.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, dos);
            EndpointState.serializer.serialize(entry.getValue(), dos, version);
        }
        dos.writeInt(ack2.msgId);
        dos.writeInt(ack2.syncId);
        dos.writeInt(ack2.ackId);
        dos.writeLong(ack2.createdTime);
    }

    public GossipDigestAck2 deserialize(DataInput dis, int version) throws IOException
    {
        int size = dis.readInt();
        Map<InetAddress, EndpointState> epStateMap = new HashMap<InetAddress, EndpointState>(size);

        for (int i = 0; i < size; ++i)
        {
            InetAddress ep = CompactEndpointSerializationHelper.deserialize(dis);
            EndpointState epState = EndpointState.serializer.deserialize(dis, version);
            epStateMap.put(ep, epState);
        }
        int msgId = dis.readInt();
        int syncId = dis.readInt();
        int ackId = dis.readInt();
        long createdTime = dis.readLong();
        GossipDigestAck2 ack2 = new GossipDigestAck2(epStateMap, msgId, syncId, ackId);
        ack2.setCreatedTime(createdTime);
        return ack2;
    }

    public long serializedSize(GossipDigestAck2 ack2, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(ack2.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack2.epStateMap.entrySet())
            size += CompactEndpointSerializationHelper.serializedSize(entry.getKey())
                  + EndpointState.serializer.serializedSize(entry.getValue(), version);
        return size;
    }
}

