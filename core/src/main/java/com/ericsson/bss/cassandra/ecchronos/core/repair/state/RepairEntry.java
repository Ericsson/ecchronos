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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * An entry from the repair history.
 */
public class RepairEntry
{
    private final LongTokenRange myRange;
    private final long myStartedAt;
    private final Set<DriverNode> myParticipants;
    private final RepairStatus myStatus;
    private final long myFinishedAt;

    /**
     * Constructor.
     *
     * @param range Token range.
     * @param startedAt Start timestamp.
     * @param finishedAt End timestamp.
     * @param participants Node participants.
     * @param status The status.
     */
    public RepairEntry(final LongTokenRange range,
                       final long startedAt,
                       final long finishedAt,
                       final Set<DriverNode> participants,
                       final String status)
    {
        myRange = range;
        myStartedAt = startedAt;
        myFinishedAt = finishedAt;
        myParticipants = Collections.unmodifiableSet(participants);
        myStatus = RepairStatus.getFromStatus(status);
    }

    /**
     * Get range.
     *
     * @return LongTokenRange
     */
    public LongTokenRange getRange()
    {
        return myRange;
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
     * Get finished at.
     *
     * @return long
     */
    public long getFinishedAt()
    {
        return myFinishedAt;
    }

    /**
     * Get participants.
     *
     * @return The participants
     */
    public Set<DriverNode> getParticipants()
    {
        return myParticipants;
    }

    /**
     * Get status.
     *
     * @return RepairStatus
     */
    public RepairStatus getStatus()
    {
        return myStatus;
    }

    /**
     * Equality.
     *
     * @param o The object to test equality with.
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
        RepairEntry that = (RepairEntry) o;
        return myStartedAt == that.myStartedAt
                && myFinishedAt == that.myFinishedAt
                && Objects.equals(myRange, that.myRange)
                && Objects.equals(myParticipants, that.myParticipants)
                && myStatus == that.myStatus;
    }

    /**
     * Hash representation.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myRange, myStartedAt, myFinishedAt, myParticipants, myStatus);
    }
}
