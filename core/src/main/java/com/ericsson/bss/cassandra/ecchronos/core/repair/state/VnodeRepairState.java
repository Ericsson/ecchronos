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
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;

import java.util.Objects;
import java.util.Set;

/**
 * A class representing the repair state of a single vnode.
 */
public class VnodeRepairState
{
    public static final long UNREPAIRED = -1L;

    private final LongTokenRange myTokenRange;
    private final Set<DriverNode> myReplicas;
    private final long myStartedAt;
    private final long myFinishedAt;
    private final long myRepairTime;

    /**
     * Constructor.
     *
     * @param tokenRange The token range.
     * @param replicas The nodes.
     * @param startedAt Started at timetamp.
     */
    public VnodeRepairState(final LongTokenRange tokenRange,
                            final Set<DriverNode> replicas,
                            final long startedAt)
    {
        this(tokenRange, replicas, startedAt, UNREPAIRED);
    }

    /**
     * Constructor.
     *
     * @param tokenRange The token range.
     * @param replicas The nodes.
     * @param startedAt Started at timestamp.
     * @param finishedAt Finished at timestamp.
     * @param repairTime Repair time.
     */
    public VnodeRepairState(final LongTokenRange tokenRange,
                            final Set<DriverNode> replicas,
                            final long startedAt,
                            final long finishedAt,
                            final long repairTime)
    {
        myTokenRange = tokenRange;
        myReplicas = replicas;
        myStartedAt = startedAt;
        myFinishedAt = finishedAt;
        myRepairTime = repairTime;
    }

    /**
     * Constructor.
     *
     * @param tokenRange The token range.
     * @param replicas The nodes.
     * @param startedAt Started at timestamp.
     * @param finishedAt Finished at timestamp.
     */
    public VnodeRepairState(final LongTokenRange tokenRange,
                            final Set<DriverNode> replicas,
                            final long startedAt,
                            final long finishedAt)
    {
        myTokenRange = tokenRange;
        myReplicas = replicas;
        myStartedAt = startedAt;
        myFinishedAt = finishedAt;
        if (myFinishedAt != UNREPAIRED)
        {
            myRepairTime = myFinishedAt - myStartedAt;
        }
        else
        {
            myRepairTime = 0;
        }

    }

    /**
     * Get token range.
     *
     * @return LongTokenRange
     */
    public LongTokenRange getTokenRange()
    {
        return myTokenRange;
    }

    /**
     * Get replicas.
     *
     * @return The nodes
     */
    public Set<DriverNode> getReplicas()
    {
        return myReplicas;
    }

    /**
     * Get last repaired at.
     *
     * @return long
     */
    public long lastRepairedAt()
    {
        return myStartedAt;
    }

    /**
     * Get finished at.
     *
     * @return long
     */
    public long getFinishedAt()
    {
        return myFinishedAt;
    }

    /**
     * Get started at.
     *
     * @return long
     */
    public long getStartedAt()
    {
        return myStartedAt;
    }

    /**
     * Get repair time.
     *
     * @return long
     */
    public long getRepairTime()
    {
        return myRepairTime;
    }

    /**
     * Check if the vnodes are the same.
     *
     * The vnodes are the same if both token range and replicas match.
     *
     * @param other The vnode to compare to.
     * @return True if it represents the same vnode.
     */
    public boolean isSameVnode(final VnodeRepairState other)
    {
        return getTokenRange().equals(other.getTokenRange()) && getReplicas().equals(other.getReplicas());
    }

    /**
     * Returns a string representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return "VnodeRepairState{"
                + "myTokenRange=" + myTokenRange
                + ", myReplicas=" + myReplicas
                + ", myStartedAt=" + myStartedAt
                + ", myFinishedAt=" + myFinishedAt
                + ", myRepairTime=" + myRepairTime
                + '}';
    }

    /**
     * Checks equality.
     *
     * @param o Object to compare to.
     * @return boolean
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        VnodeRepairState that = (VnodeRepairState) o;
        return myStartedAt == that.myStartedAt
                && myFinishedAt == that.myFinishedAt
                && myRepairTime == that.myRepairTime
                && Objects.equals(myTokenRange, that.myTokenRange)
                && Objects.equals(myReplicas, that.myReplicas);
    }

    /**
     * Return a hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myTokenRange, myReplicas, myStartedAt, myFinishedAt, myRepairTime);
    }
}
