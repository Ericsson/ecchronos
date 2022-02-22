/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import java.util.UUID;

public class OngoingRepair
{
    public enum Status
    {
        started, finished, failed
    }

    private final UUID myRepairId;
    private final TableReference myTableReference;
    private final OngoingRepair.Status myStatus;
    private final String myTriggeredBy;
    private final long myFinishedAt;
    private final long myStartedAt;
    private final int myRemainingTasks;

    private OngoingRepair(OngoingRepair.Builder builder)
    {
        myRepairId = builder.myRepairId == null ? UUID.randomUUID() : builder.myRepairId;
        myTableReference = builder.myTableReference;
        myStatus = builder.myStatus;
        myFinishedAt = builder.myFinishedAt;
        myStartedAt = builder.myStartedAt;
        myTriggeredBy = builder.myTriggeredBy;
        myRemainingTasks = builder.myRemainingTasks;
    }

    public UUID getRepairId()
    {
        return myRepairId;
    }

    public OngoingRepair.Status getStatus()
    {
        return myStatus;
    }

    public long getFinishedAt()
    {
        return myFinishedAt;
    }

    public long getStartedAt()
    {
        return myStartedAt;
    }

    public TableReference getTableReference()
    {
        return myTableReference;
    }

    public String getTriggeredBy()
    {
        return myTriggeredBy;
    }

    public int getRemainingTasks()
    {
        return myRemainingTasks;
    }

    public static class Builder
    {
        private UUID myRepairId = null;
        private TableReference myTableReference;
        private OngoingRepair.Status myStatus = OngoingRepair.Status.started;
        private long myFinishedAt = -1;
        private long myStartedAt = -1;
        private String myTriggeredBy;
        private int myRemainingTasks = -1;

        public Builder withOngoingRepairInfo(UUID repairId, OngoingRepair.Status status, Long finishedAt, Long startedAt, String triggeredBy, int remainingTasks)
        {
            myRepairId = repairId;
            myStatus = status;
            if(finishedAt != null)
            {
                myFinishedAt = finishedAt;
            }
            if(startedAt != null)
            {
                this.myStartedAt = startedAt;
            }
            myTriggeredBy = triggeredBy;
            myRemainingTasks = remainingTasks;
            return this;
        }

        public Builder withTableReference(TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        public OngoingRepair build()
        {
            return new OngoingRepair(this);
        }
    }
}
