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
    private final Set<Node> myParticipants;
    private final RepairStatus myStatus;

    public RepairEntry(LongTokenRange range, long startedAt, Set<Node> participants, String status)
    {
        myRange = range;
        myStartedAt = startedAt;
        myParticipants = Collections.unmodifiableSet(participants);
        myStatus = RepairStatus.getFromStatus(status);
    }

    public LongTokenRange getRange()
    {
        return myRange;
    }

    public long getStartedAt()
    {
        return myStartedAt;
    }

    public Set<Node> getParticipants()
    {
        return myParticipants;
    }

    public RepairStatus getStatus()
    {
        return myStatus;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairEntry that = (RepairEntry) o;
        return myStartedAt == that.myStartedAt &&
                Objects.equals(myRange, that.myRange) &&
                Objects.equals(myParticipants, that.myParticipants) &&
                myStatus == that.myStatus;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(myRange, myStartedAt, myParticipants, myStatus);
    }
}
