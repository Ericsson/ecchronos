/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.data.repairhistory;

import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public final class RepairHistoryData
{
    private final UUID myTableId;
    private final UUID myNodeId;
    private final UUID myRepairId;
    private final UUID myJobId;
    private final UUID myCoordinatorId;
    private final String myRangeBegin;
    private final String myRangeEnd;
    private final Set<UUID> myParticipants;
    private final RepairStatus myStatus;
    private final Instant myStartedAt;
    private final Instant myFinishedAt;
    private final long myLookBackTimeInMs;

    private RepairHistoryData(final Builder builder)
    {
        this.myTableId = builder.myTableId;
        this.myNodeId = builder.myNodeId;
        this.myRepairId = builder.myRepairId;
        this.myJobId = builder.myJobId;
        this.myCoordinatorId = builder.myCoordinatorId;
        this.myRangeBegin = builder.myRangeBegin;
        this.myRangeEnd = builder.myRangeEnd;
        this.myParticipants = builder.myParticipants == null ? Set.of() : Set.copyOf(builder.myParticipants);
        this.myStatus = builder.myStatus;
        this.myStartedAt = builder.myStartedAt;
        this.myFinishedAt = builder.myFinishedAt;
        this.myLookBackTimeInMs = builder.myLookBackTimeInMs;
    }

    public UUID getTableId()
    {
        return myTableId;
    }

    public UUID getNodeId()
    {
        return myNodeId;
    }

    public UUID getJobId()
    {
        return myJobId;
    }

    public UUID getRepairId()
    {
        return myRepairId;
    }

    public UUID getCoordinatorId()
    {
        return myCoordinatorId;
    }

    public String getRangeBegin()
    {
        return myRangeBegin;
    }

    public String getRangeEnd()
    {
        return myRangeEnd;
    }

    public Set<UUID> getParticipants()
    {
        return myParticipants;
    }

    public RepairStatus getStatus()
    {
        return myStatus;
    }

    public Instant getStartedAt()
    {
        return myStartedAt;
    }

    public Instant getFinishedAt()
    {
        return myFinishedAt;
    }

    public long getLookBackTimeInMilliseconds()
    {
        return myLookBackTimeInMs;
    }

    public static Builder copyOf(final RepairHistoryData repairHistoryData)
    {
        return new RepairHistoryData.Builder()
                .withTableId(repairHistoryData.myTableId)
                .withNodeId(repairHistoryData.myNodeId)
                .withRepairId(repairHistoryData.myRepairId)
                .withJobId(repairHistoryData.myJobId)
                .withCoordinatorId(repairHistoryData.myCoordinatorId)
                .withRangeBegin(repairHistoryData.myRangeBegin)
                .withRangeEnd(repairHistoryData.myRangeEnd)
                .withParticipants(repairHistoryData.myParticipants)
                .withStatus(repairHistoryData.myStatus)
                .withStartedAt(repairHistoryData.myStartedAt)
                .withFinishedAt(repairHistoryData.myFinishedAt)
                .withLookBackTimeInMilliseconds(repairHistoryData.myLookBackTimeInMs);
    }

    public static final class Builder
    {
        private UUID myTableId;
        private UUID myNodeId;
        private UUID myRepairId;
        private UUID myJobId;
        private UUID myCoordinatorId;
        private String myRangeBegin;
        private String myRangeEnd;
        private Set<UUID> myParticipants;
        private RepairStatus myStatus;
        private Instant myStartedAt;
        private Instant myFinishedAt;
        private long myLookBackTimeInMs;

        public Builder withTableId(final UUID tableId)
        {
            this.myTableId = tableId;
            return this;
        }

        public Builder withNodeId(final UUID nodeId)
        {
            this.myNodeId = nodeId;
            return this;
        }

        public Builder withRepairId(final UUID repairId)
        {
            this.myRepairId = repairId;
            return this;
        }

        public Builder withJobId(final UUID jobId)
        {
            this.myJobId = jobId;
            return this;
        }

        public Builder withCoordinatorId(final UUID coordinatorId)
        {
            this.myCoordinatorId = coordinatorId;
            return this;
        }

        public Builder withRangeBegin(final String rangeBegin)
        {
            this.myRangeBegin = rangeBegin;
            return this;
        }

        public Builder withRangeEnd(final String rangeEnd)
        {
            this.myRangeEnd = rangeEnd;
            return this;
        }

        public Builder withParticipants(final Set<UUID> participants)
        {
            this.myParticipants = (participants == null) ? Set.of() : new HashSet<>(participants);
            return this;
        }

        public Builder withStatus(final RepairStatus status)
        {
            this.myStatus = status;
            return this;
        }

        public Builder withStartedAt(final Instant startedAt)
        {
            this.myStartedAt = startedAt;
            return this;
        }

        public Builder withFinishedAt(final Instant finishedAt)
        {
            this.myFinishedAt = finishedAt;
            return this;
        }

        public Builder withLookBackTimeInMilliseconds(final long lookBackTimeInMilliseconds)
        {
            this.myLookBackTimeInMs = lookBackTimeInMilliseconds;
            return this;
        }

        public RepairHistoryData build()
        {
            Preconditions.checkNotNull(myTableId, "Table ID cannot be null");
            Preconditions.checkNotNull(myNodeId, "Node ID cannot be null");
            Preconditions.checkNotNull(myRepairId, "Repair ID cannot be null");
            Preconditions.checkNotNull(myStatus, "Status cannot be null");
            Preconditions.checkNotNull(myStartedAt, "StartedAt cannot be null");
            Preconditions.checkNotNull(myFinishedAt, "FinishedAt cannot be null");
            Preconditions.checkArgument(myLookBackTimeInMs > 0, "LookBack time must be a positive number");
            return new RepairHistoryData(this);
        }
    }
}
