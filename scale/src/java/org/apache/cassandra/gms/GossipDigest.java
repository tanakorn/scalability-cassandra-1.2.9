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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
public class GossipDigest implements Comparable<GossipDigest>,  Serializable
{
    public static final IVersionedSerializer<GossipDigest> serializer = new GossipDigestSerializer();

    final InetAddress endpoint;
    final int generation;
    final int maxVersion;

    public GossipDigest(InetAddress ep, int gen, int version)
    {
        endpoint = ep;
        generation = gen;
        maxVersion = version;
    }

    public InetAddress getEndpoint()
    {
        return endpoint;
    }

    public int getGeneration()
    {
        return generation;
    }

    public int getMaxVersion()
    {
        return maxVersion;
    }

    public int compareTo(GossipDigest gDigest)
    {
        if ( generation != gDigest.generation )
            return ( generation - gDigest.generation );
        return (maxVersion - gDigest.maxVersion);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((endpoint == null) ? 0 : endpoint.hashCode());
        result = prime * result + generation;
        result = prime * result + maxVersion;
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
        GossipDigest other = (GossipDigest) obj;
        if (endpoint == null) {
            if (other.endpoint != null)
                return false;
        } else if (!endpoint.equals(other.endpoint))
            return false;
        if (generation != other.generation)
            return false;
        if (maxVersion != other.maxVersion)
            return false;
        return true;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(endpoint);
        sb.append(":");
        sb.append(generation);
        sb.append(":");
        sb.append(maxVersion);
        return sb.toString();
    }
}

class GossipDigestSerializer implements IVersionedSerializer<GossipDigest>
{
    public void serialize(GossipDigest gDigest, DataOutput dos, int version) throws IOException
    {
        CompactEndpointSerializationHelper.serialize(gDigest.endpoint, dos);
        dos.writeInt(gDigest.generation);
        dos.writeInt(gDigest.maxVersion);
    }

    public GossipDigest deserialize(DataInput dis, int version) throws IOException
    {
        InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(dis);
        int generation = dis.readInt();
        int maxVersion = dis.readInt();
        return new GossipDigest(endpoint, generation, maxVersion);
    }

    public long serializedSize(GossipDigest gDigest, int version)
    {
        long size = CompactEndpointSerializationHelper.serializedSize(gDigest.endpoint);
        size += TypeSizes.NATIVE.sizeof(gDigest.generation);
        size += TypeSizes.NATIVE.sizeof(gDigest.maxVersion);
        return size;
    }
}
