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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
public class GossipDigestAck
{
    public static final IVersionedSerializer<GossipDigestAck> serializer = new GossipDigestAckSerializer();

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    final List<GossipDigest> gDigestList;
    final Map<InetAddress, EndpointState> epStateMap;
    final int msgId;
    final int syncId;

    GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddress, EndpointState> epStateMap, int syncId)
    {
        this.gDigestList = gDigestList;
        this.epStateMap = epStateMap;
        this.msgId = idGenerator.getAndIncrement();
        this.syncId = syncId;
    }

    GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddress, EndpointState> epStateMap, int msgId, int syncId)
    {
        this.gDigestList = gDigestList;
        this.epStateMap = epStateMap;
        this.msgId = msgId;
        this.syncId = syncId;
    }

    List<GossipDigest> getGossipDigestList()
    {
        return gDigestList;
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
		result = prime * result
				+ ((gDigestList == null) ? 0 : gDigestList.hashCode());
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
		GossipDigestAck other = (GossipDigestAck) obj;
		if (epStateMap == null) {
			if (other.epStateMap != null)
				return false;
		} else if (!epStateMap.equals(other.epStateMap))
			return false;
		if (gDigestList == null) {
			if (other.gDigestList != null)
				return false;
		} else if (!gDigestList.equals(other.gDigestList))
			return false;
		return true;
	}

    public int getMsgId() {
        return msgId;
    }

}

class GossipDigestAckSerializer implements IVersionedSerializer<GossipDigestAck>
{
    public void serialize(GossipDigestAck gDigestAckMessage, DataOutput dos, int version) throws IOException
    {
        GossipDigestSerializationHelper.serialize(gDigestAckMessage.gDigestList, dos, version);
        if (version < MessagingService.VERSION_12)
            dos.writeBoolean(true); // 0.6 compatibility
        dos.writeInt(gDigestAckMessage.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : gDigestAckMessage.epStateMap.entrySet())
        {
            InetAddress ep = entry.getKey();
            CompactEndpointSerializationHelper.serialize(ep, dos);
            EndpointState.serializer.serialize(entry.getValue(), dos, version);
        }
        dos.writeInt(gDigestAckMessage.msgId);
        dos.writeInt(gDigestAckMessage.syncId);
    }

    public GossipDigestAck deserialize(DataInput dis, int version) throws IOException
    {
        List<GossipDigest> gDigestList = GossipDigestSerializationHelper.deserialize(dis, version);
        if (version < MessagingService.VERSION_12)
            dis.readBoolean(); // 0.6 compatibility
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
        GossipDigestAck ack = new GossipDigestAck(gDigestList, epStateMap, msgId, syncId);
        return ack;
    }

    public long serializedSize(GossipDigestAck ack, int version)
    {
        int size = GossipDigestSerializationHelper.serializedSize(ack.gDigestList, version);
        if (version < MessagingService.VERSION_12)
            size += TypeSizes.NATIVE.sizeof(true);
        size += TypeSizes.NATIVE.sizeof(ack.epStateMap.size());
        for (Map.Entry<InetAddress, EndpointState> entry : ack.epStateMap.entrySet())
            size += CompactEndpointSerializationHelper.serializedSize(entry.getKey())
                  + EndpointState.serializer.serializedSize(entry.getValue(), version);
        return size;
    }
}
