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

/**
 * Immutable data class representing a single repair history entry.
 * Contains information about the table, node, range, participants, status, and timestamps.
 */
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

    /**
     * Gets the table identifier.
     *
     * @return the table UUID.
     */
    public UUID getTableId()
    {
        return myTableId;
    }

    /**
     * Gets the node identifier.
     *
     * @return the node UUID.
     */
    public UUID getNodeId()
    {
        return myNodeId;
    }

    /**
     * Gets the job identifier.
     *
     * @return the job UUID.
     */
    public UUID getJobId()
    {
        return myJobId;
    }

    /**
     * Gets the repair identifier.
     *
     * @return the repair UUID.
     */
    public UUID getRepairId()
    {
        return myRepairId;
    }

    /**
     * Gets the coordinator node identifier.
     *
     * @return the coordinator UUID.
     */
    public UUID getCoordinatorId()
    {
        return myCoordinatorId;
    }

    /**
     * Gets the beginning of the token range.
     *
     * @return the range begin as a string.
     */
    public String getRangeBegin()
    {
        return myRangeBegin;
    }

    /**
     * Gets the end of the token range.
     *
     * @return the range end as a string.
     */
    public String getRangeEnd()
    {
        return myRangeEnd;
    }

    /**
     * Gets the set of participant node identifiers in this repair.
     *
     * @return an immutable set of participant UUIDs.
     */
    public Set<UUID> getParticipants()
    {
        return myParticipants;
    }

    /**
     * Gets the repair status.
     *
     * @return the repair status.
     */
    public RepairStatus getStatus()
    {
        return myStatus;
    }

    /**
     * Gets the timestamp when the repair started.
     *
     * @return the started-at instant.
     */
    public Instant getStartedAt()
    {
        return myStartedAt;
    }

    /**
     * Gets the timestamp when the repair finished.
     *
     * @return the finished-at instant.
     */
    public Instant getFinishedAt()
    {
        return myFinishedAt;
    }

    /**
     * Gets the look-back time in milliseconds used for querying repair history.
     *
     * @return the look-back time in milliseconds.
     */
    public long getLookBackTimeInMilliseconds()
    {
        return myLookBackTimeInMs;
    }

    /**
     * Creates a new builder pre-populated with values from the given repair history data.
     *
     * @param repairHistoryData the data to copy values from.
     * @return a new pre-populated builder.
     */
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

    /**
     * Builder for constructing {@link RepairHistoryData} instances.
     */
    public static final class Builder
    {
        /**
         * Default constructor.
         */
        public Builder()
        {
            // Default constructor
        }

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

        /**
         * Sets the table identifier.
         *
         * @param tableId the table UUID.
         * @return this builder.
         */
        public Builder withTableId(final UUID tableId)
        {
            this.myTableId = tableId;
            return this;
        }

        /**
         * Sets the node identifier.
         *
         * @param nodeId the node UUID.
         * @return this builder.
         */
        public Builder withNodeId(final UUID nodeId)
        {
            this.myNodeId = nodeId;
            return this;
        }

        /**
         * Sets the repair identifier.
         *
         * @param repairId the repair UUID.
         * @return this builder.
         */
        public Builder withRepairId(final UUID repairId)
        {
            this.myRepairId = repairId;
            return this;
        }

        /**
         * Sets the job identifier.
         *
         * @param jobId the job UUID.
         * @return this builder.
         */
        public Builder withJobId(final UUID jobId)
        {
            this.myJobId = jobId;
            return this;
        }

        /**
         * Sets the coordinator node identifier.
         *
         * @param coordinatorId the coordinator UUID.
         * @return this builder.
         */
        public Builder withCoordinatorId(final UUID coordinatorId)
        {
            this.myCoordinatorId = coordinatorId;
            return this;
        }

        /**
         * Sets the beginning of the token range.
         *
         * @param rangeBegin the range begin as a string.
         * @return this builder.
         */
        public Builder withRangeBegin(final String rangeBegin)
        {
            this.myRangeBegin = rangeBegin;
            return this;
        }

        /**
         * Sets the end of the token range.
         *
         * @param rangeEnd the range end as a string.
         * @return this builder.
         */
        public Builder withRangeEnd(final String rangeEnd)
        {
            this.myRangeEnd = rangeEnd;
            return this;
        }

        /**
         * Sets the set of participant node identifiers.
         *
         * @param participants the set of participant UUIDs.
         * @return this builder.
         */
        public Builder withParticipants(final Set<UUID> participants)
        {
            this.myParticipants = (participants == null) ? Set.of() : new HashSet<>(participants);
            return this;
        }

        /**
         * Sets the repair status.
         *
         * @param status the repair status.
         * @return this builder.
         */
        public Builder withStatus(final RepairStatus status)
        {
            this.myStatus = status;
            return this;
        }

        /**
         * Sets the timestamp when the repair started.
         *
         * @param startedAt the started-at instant.
         * @return this builder.
         */
        public Builder withStartedAt(final Instant startedAt)
        {
            this.myStartedAt = startedAt;
            return this;
        }

        /**
         * Sets the timestamp when the repair finished.
         *
         * @param finishedAt the finished-at instant.
         * @return this builder.
         */
        public Builder withFinishedAt(final Instant finishedAt)
        {
            this.myFinishedAt = finishedAt;
            return this;
        }

        /**
         * Sets the look-back time in milliseconds.
         *
         * @param lookBackTimeInMilliseconds the look-back time.
         * @return this builder.
         */
        public Builder withLookBackTimeInMilliseconds(final long lookBackTimeInMilliseconds)
        {
            this.myLookBackTimeInMs = lookBackTimeInMilliseconds;
            return this;
        }

        /**
         * Builds the {@link RepairHistoryData} instance.
         *
         * @return the constructed repair history data.
         * @throws NullPointerException if required fields are null.
         * @throws IllegalArgumentException if look-back time is not positive.
         */
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
