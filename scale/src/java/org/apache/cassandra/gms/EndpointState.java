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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */


public class EndpointState
{
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    public final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();

    private volatile HeartBeatState hbState;
    final Map<ApplicationState, VersionedValue> applicationState = new NonBlockingHashMap<ApplicationState, VersionedValue>();

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    int hopNum;

    public EndpointState(HeartBeatState initialHbState)
    {
        hbState = initialHbState;
        updateTimestamp = System.currentTimeMillis();
        isAlive = true;
        hopNum = 0;
    }
    
    public EndpointState(HeartBeatState initialHbState, long updateTimestamp, boolean isAlive)
    {
        hbState = initialHbState;
        this.updateTimestamp = updateTimestamp;
        this.isAlive = isAlive;
    }

    public HeartBeatState getHeartBeatState()
    {
        return hbState;
    }

    public void setHeartBeatState(HeartBeatState newHbState)
    {
        updateTimestamp();
        hbState = newHbState;
    }

    public int getHopNum() {
        return hopNum;
    }

    public void setHopNum(int hopNum) {
        this.hopNum = hopNum;
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return applicationState.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    @Deprecated
    public Map<ApplicationState, VersionedValue> getApplicationStateMap()
    {
        return applicationState;
    }

    public void addApplicationState(ApplicationState key, VersionedValue value)
    {
        applicationState.put(key, value);
    }

    /* getters and setters */
    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }

    public void updateTimestamp()
    {
        updateTimestamp = System.currentTimeMillis();
    }

    public boolean isAlive()
    {
        return isAlive;
    }

    public void markAlive()
    {
        isAlive = true;
    }

    public void markDead()
    {
        isAlive = false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((applicationState == null) ? 0 : applicationState.hashCode());
        result = prime * result + ((hbState == null) ? 0 : hbState.hashCode());
        result = prime * result + (isAlive ? 1231 : 1237);
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
        EndpointState other = (EndpointState) obj;
        if (applicationState == null) {
            if (other.applicationState != null)
                return false;
        } else if (!applicationState.equals(other.applicationState))
            return false;
        if (hbState == null) {
            if (other.hbState != null)
                return false;
        } else if (!hbState.equals(other.hbState))
            return false;
        if (isAlive != other.isAlive)
            return false;
        return true;
    }

    public String toString()
    {
        return "EndpointState: HeartBeatState = " + hbState + ", AppStateMap = " + applicationState;
    }
    
    public EndpointState copy() {
        EndpointState clone = new EndpointState(hbState.copy(), updateTimestamp, isAlive);
        for (ApplicationState state : applicationState.keySet()) {
            VersionedValue vv = applicationState.get(state);
            clone.applicationState.put(state, vv.copy());
        }
        return clone;
    }
}

class EndpointStateSerializer implements IVersionedSerializer<EndpointState>
{
    public void serialize(EndpointState epState, DataOutput dos, int version) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer.serialize(hbState, dos, version);

        /* serialize the map of ApplicationState objects */
        int size = epState.applicationState.size();
        dos.writeInt(size);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.applicationState.entrySet())
        {
            VersionedValue value = entry.getValue();
            dos.writeInt(entry.getKey().ordinal());
            VersionedValue.serializer.serialize(value, dos, version);
        }
        dos.writeInt(epState.hopNum);
    }

    public EndpointState deserialize(DataInput dis, int version) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer.deserialize(dis, version);
        EndpointState epState = new EndpointState(hbState);

        int appStateSize = dis.readInt();
        for ( int i = 0; i < appStateSize; ++i )
        {
            int key = dis.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(dis, version);
            epState.addApplicationState(Gossiper.STATES[key], value);
        }
        int hopNum = dis.readInt();
        epState.hopNum = hopNum;
        return epState;
    }

    public long serializedSize(EndpointState epState, int version)
    {
        long size = HeartBeatState.serializer.serializedSize(epState.getHeartBeatState(), version);
        size += TypeSizes.NATIVE.sizeof(epState.applicationState.size());
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.applicationState.entrySet())
        {
            VersionedValue value = entry.getValue();
            size += TypeSizes.NATIVE.sizeof(entry.getKey().ordinal());
            size += VersionedValue.serializer.serializedSize(value, version);
        }
        return size;
    }
}
