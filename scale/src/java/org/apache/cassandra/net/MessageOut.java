/**
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

package org.apache.cassandra.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.tracing.Tracing.TRACE_HEADER;
import static org.apache.cassandra.tracing.Tracing.isTracing;

public class MessageOut<T>
{
    public final InetAddress from;
    public final InetAddress to;
    public final MessagingService.Verb verb;
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    public final Map<String, byte[]> parameters;
    
    public long wakeUpTime;
    
    // we do support messages that just consist of a verb
    public MessageOut(MessagingService.Verb verb)
    {
        this(verb, null, null);
    }

    public MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer)
    {
        this(verb,
             payload,
             serializer,
             isTracing() ? ImmutableMap.of(TRACE_HEADER, UUIDGen.decompose(Tracing.instance().getSessionId()))
                         : Collections.<String, byte[]>emptyMap());
    }

    public MessageOut(InetAddress from, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer)
    {
        this(from, verb,
             payload,
             serializer,
             isTracing() ? ImmutableMap.of(TRACE_HEADER, UUIDGen.decompose(Tracing.instance().getSessionId()))
                         : Collections.<String, byte[]>emptyMap());
    }
    
    public MessageOut(InetAddress from, InetAddress to, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer)
    {
        this(from, to, verb,
             payload,
             serializer,
             isTracing() ? ImmutableMap.of(TRACE_HEADER, UUIDGen.decompose(Tracing.instance().getSessionId()))
                         : Collections.<String, byte[]>emptyMap());
    }

    private MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this(FBUtilities.getBroadcastAddress(), verb, payload, serializer, parameters);
    }

    public MessageOut(InetAddress from, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this.from = from;
        this.to = null;
        this.verb = verb;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
    }

    public MessageOut(InetAddress from, InetAddress to, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this.from = from;
        this.to = to;
        this.verb = verb;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
    }

    public MessageOut<T> withParameter(String key, byte[] value)
    {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        builder.putAll(parameters).put(key, value);
        return new MessageOut<T>(verb, payload, serializer, builder.build());
    }
    
    public MessageOut<T> copy() {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        builder.putAll(parameters);
    	MessageOut<T> msg = new MessageOut<T>(from, verb, payload, serializer, builder.build());
    	return msg;
    }

    public Stage getStage()
    {
        return MessagingService.verbStages.get(verb);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getTimeout(verb);
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder("");
        sbuf.append("TYPE:").append(getStage()).append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public void serialize(DataOutputStream out, int version) throws IOException
    {
        CompactEndpointSerializationHelper.serialize(from, out);

        out.writeInt(verb.ordinal());
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        long longSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        out.writeInt((int) longSize);
        if (payload != null)
            serializer.serialize(payload, out, version);
    }

    public int serializedSize(int version)
    {
        int size = CompactEndpointSerializationHelper.serializedSize(from);

        size += TypeSizes.NATIVE.sizeof(verb.ordinal());
        size += TypeSizes.NATIVE.sizeof(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            TypeSizes.NATIVE.sizeof(entry.getKey());
            TypeSizes.NATIVE.sizeof(entry.getValue().length);
            size += entry.getValue().length;
        }

        long longSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += TypeSizes.NATIVE.sizeof((int) longSize);
        size += longSize;
        return size;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result
				+ ((parameters == null) ? 0 : parameters.hashCode());
		result = prime * result + ((payload == null) ? 0 : payload.hashCode());
		result = prime * result
				+ ((serializer == null) ? 0 : serializer.hashCode());
		result = prime * result + ((verb == null) ? 0 : verb.hashCode());
		return result;
	}

	public long getWakeUpTime() {
        return wakeUpTime;
    }

    public void setWakeUpTime(long wakeUpTime) {
        this.wakeUpTime = wakeUpTime;
    }

    @Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MessageOut other = (MessageOut) obj;
		if (from == null) {
			if (other.from != null)
				return false;
		} else if (!from.equals(other.from))
			return false;
		if (parameters == null) {
			if (other.parameters != null)
				return false;
		} else if (!parameters.equals(other.parameters))
			return false;
		if (payload == null) {
			if (other.payload != null)
				return false;
		} else if (!payload.equals(other.payload))
			return false;
		if (serializer == null) {
			if (other.serializer != null)
				return false;
		} else if (!serializer.equals(other.serializer))
			return false;
		if (verb != other.verb)
			return false;
		return true;
	}
    
    
}
