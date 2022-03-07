/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

/**
 * A class representing the repair state of a single vnode.
 */
public class VnodeRepairState
{
    public static final long UNREPAIRED = -1L;

    private final LongTokenRange myTokenRange;
    private final ImmutableSet<Node> myReplicas;
    private final long myStartedAt;
    private final long myFinishedAt;

    public VnodeRepairState(LongTokenRange tokenRange, ImmutableSet<Node> replicas, long startedAt)
    {
        this(tokenRange, replicas, startedAt, UNREPAIRED);
    }

    public VnodeRepairState(LongTokenRange tokenRange, ImmutableSet<Node> replicas, long startedAt, long finishedAt)
    {
        myTokenRange = tokenRange;
        myReplicas = replicas;
        myStartedAt = startedAt;
        myFinishedAt = finishedAt;
    }

    public LongTokenRange getTokenRange()
    {
        return myTokenRange;
    }

    public ImmutableSet<Node> getReplicas()
    {
        return myReplicas;
    }

    public long lastRepairedAt()
    {
        return myStartedAt;
    }

    public long getFinishedAt()
    {
        return myFinishedAt;
    }

    public long getStartedAt()
    {
        return myStartedAt;
    }

    /**
     * Check if the vnodes are the same.
     *
     * The vnodes are the same if both token range and replicas match.
     *
     * @param other The vnode to compare to.
     * @return True if it represents the same vnode.
     */
    public boolean isSameVnode(VnodeRepairState other)
    {
        return getTokenRange().equals(other.getTokenRange()) && getReplicas().equals(other.getReplicas());
    }

    @Override
    public String toString()
    {
        return "VnodeRepairState{" +
                "myTokenRange=" + myTokenRange +
                ", myReplicas=" + myReplicas +
                ", myStartedAt=" + myStartedAt +
                ", myFinishedAt=" + myFinishedAt +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VnodeRepairState that = (VnodeRepairState) o;
        return myStartedAt == that.myStartedAt &&
                myFinishedAt == that.myFinishedAt &&
                Objects.equals(myTokenRange, that.myTokenRange) &&
                Objects.equals(myReplicas, that.myReplicas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myTokenRange, myReplicas, myStartedAt, myFinishedAt);
    }
}
