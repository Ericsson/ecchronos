/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.Objects;
import java.util.UUID;

public class OnDemandRepairJobView
{
    public enum Status
    {
        COMPLETED, IN_QUEUE, WARNING, ERROR, BLOCKED
    }

    private final UUID myId;
    private final TableReference myTableReference;
    private final Status myStatus;
    private final double myProgress;
    private final UUID myHostId;
    private final long myCompletionTime;

    public OnDemandRepairJobView(final UUID id,
                                 final UUID hostId,
                                 final TableReference tableReference,
                                 final Status status,
                                 final double progress,
                                 final long completionTime)
    {
        myId = id;
        myTableReference = tableReference;
        myStatus = status;
        myProgress = progress;
        myHostId = hostId;
        myCompletionTime = completionTime;
    }

    /**
     * Get id.
     *
     * @return UUID
     */
    public UUID getId()
    {
        return myId;
    }

    /**
     * Get table reference.
     *
     * @return TableReference
     */
    public TableReference getTableReference()
    {
        return myTableReference;
    }

    /**
     * Get status.
     *
     * @return Status
     */
    public Status getStatus()
    {
        return myStatus;
    }

    /**
     * Get progress.
     *
     * @return double
     */
    public double getProgress()
    {
        return myProgress;
    }

    /**
     * Get host id.
     *
     * @return UUID
     */
    public UUID getHostId()
    {
        return myHostId;
    }

    /**
    * Get completion time.
    *
    * @return long
    */
    public long getCompletionTime()
    {
        return myCompletionTime;
    }

    /**
     * Equality check.
     *
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
        OnDemandRepairJobView that = (OnDemandRepairJobView) o;
        return Double.compare(that.myProgress, myProgress) == 0 && myCompletionTime == that.myCompletionTime
                && Objects.equals(myId, that.myId) && Objects.equals(myTableReference,
                that.myTableReference) && myStatus == that.myStatus && Objects.equals(myHostId, that.myHostId);
    }

    /**
     * Hash representation of the object.
     *
     * @return int
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(myId, myTableReference, myStatus, myProgress, myHostId, myCompletionTime);
    }
}
