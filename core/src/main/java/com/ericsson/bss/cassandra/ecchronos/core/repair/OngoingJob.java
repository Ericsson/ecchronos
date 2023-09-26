/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

@SuppressWarnings("FinalClass")
public class OngoingJob
{
    public enum Status
    {
        started, finished, failed
    }

    private final UUID myJobId;
    private final UUID myHostId;
    private final TableReference myTableReference;
    private final Map<LongTokenRange, ImmutableSet<DriverNode>> myTokens;
    private final Set<UdtValue> myRepairedTokens;
    private final OnDemandStatus myOnDemandStatus;
    private final ReplicationState myReplicationState;
    private final Integer myTokenHash;
    private final Status myStatus;
    private final long myCompletedTime;
    private final RepairOptions.RepairType myRepairType;

    private OngoingJob(final Builder builder)
    {
        myOnDemandStatus = builder.myOnDemandStatus;
        myJobId = builder.myJobId == null ? UUID.randomUUID() : builder.myJobId;
        myHostId = builder.myHostId;
        myTableReference = builder.myTableReference;
        myReplicationState = builder.myReplicationState;
        myTokens = myReplicationState.getTokenRangeToReplicas(myTableReference);
        myRepairedTokens = builder.myRepairedTokens;
        myTokenHash = builder.myTokenMapHash;
        myStatus = builder.myStatus;
        myCompletedTime = builder.myCompletedTime;
        myRepairType = builder.myRepairType;

        if (myTokenHash == null)
        {
            myOnDemandStatus.addNewJob(myJobId, myTableReference, myTokens.keySet().hashCode(), myRepairType);
        }
    }

    public UUID getJobId()
    {
        return myJobId;
    }

    public UUID getHostId()
    {
        return myHostId;
    }

    public Status getStatus()
    {
        return myStatus;
    }

    public long getCompletedTime()
    {
        return myCompletedTime;
    }

    public TableReference getTableReference()
    {
        return myTableReference;
    }

    public RepairOptions.RepairType getRepairType()
    {
        return myRepairType;
    }

    public Set<LongTokenRange> getRepairedTokens()
    {
        Set<LongTokenRange> repairedLongTokenRanges = new HashSet<>();
        myRepairedTokens.forEach(t -> repairedLongTokenRanges
                .add(new LongTokenRange(myOnDemandStatus.getStartTokenFrom(t), myOnDemandStatus.getEndTokenFrom(t))));
        return repairedLongTokenRanges;
    }

    public void finishRanges(final Set<LongTokenRange> ranges)
    {
        ranges.forEach(t -> myRepairedTokens.add(myOnDemandStatus.createUDTTokenRangeValue(t.start, t.end)));
        myOnDemandStatus.updateJob(myJobId, myRepairedTokens);
    }

    public Map<LongTokenRange, ImmutableSet<DriverNode>> getTokens()
    {
        return myTokens;
    }

    public boolean hasTopologyChanged()
    {
        return !myTokens.equals(myReplicationState.getTokenRangeToReplicas(myTableReference))
                || (myTokenHash != null
                && (myTokenHash != myTokens.keySet().hashCode()
                && myTokenHash != myTokens.hashCode()));
    }

    public void startClusterWideJob(final RepairOptions.RepairType repairType)
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> allTokenRanges = myReplicationState
                .getTokenRanges(myTableReference);
        Map<DriverNode, Set<LongTokenRange>> repairedRangesPerNode = new HashMap<>();
        Map<DriverNode, Set<LongTokenRange>> remainingRangesPerNode = new HashMap<>();
        for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> range : allTokenRanges.entrySet())
        {
            LongTokenRange rangeForNodes = range.getKey();
            Set<DriverNode> nodes = range.getValue();
            boolean rangeRepaired = myTokens.containsKey(rangeForNodes);
            for (DriverNode node: nodes)
            {
                if (rangeRepaired)
                {
                    repairedRangesPerNode.computeIfAbsent(node, (k) -> new HashSet<>()).add(rangeForNodes);
                }
                else
                {
                    remainingRangesPerNode.computeIfAbsent(node, (k) -> new HashSet<>()).add(rangeForNodes);
                    rangeRepaired = true; // We only want one node to repair the range
                }
            }
        }
        for (Map.Entry<DriverNode, Set<LongTokenRange>> replicaWithRanges: remainingRangesPerNode.entrySet())
        {
            DriverNode node = replicaWithRanges.getKey();
            Set<LongTokenRange> remainingRanges = replicaWithRanges.getValue();
            Set<LongTokenRange> repairedRanges = repairedRangesPerNode.get(node);
            Set<LongTokenRange> allTokensForNode = new HashSet<>();
            if (remainingRanges != null)
            {
                allTokensForNode.addAll(remainingRanges);
            }
            if (repairedRanges != null)
            {
                allTokensForNode.addAll(repairedRanges);
            }
            if (repairType == RepairOptions.RepairType.INCREMENTAL)
            {
                myOnDemandStatus.addNewJob(node.getId(),
                        myJobId,
                        myTableReference,
                        0,
                        Collections.emptySet(), repairType);
            }
            else
            {
                myOnDemandStatus.addNewJob(node.getId(),
                        myJobId,
                        myTableReference,
                        allTokensForNode.hashCode(),
                        repairedRanges, repairType);
            }
        }
    }

    public void finishJob()
    {
        myOnDemandStatus.finishJob(myJobId);
    }

    public void failJob()
    {
        myOnDemandStatus.failJob(myJobId);
    }

    public static class Builder
    {
        private UUID myJobId = null;
        private UUID myHostId;
        private TableReference myTableReference;
        private Set<UdtValue> myRepairedTokens = new HashSet<>();
        private OnDemandStatus myOnDemandStatus;
        private ReplicationState myReplicationState;
        private Integer myTokenMapHash = null;
        private Status myStatus = Status.started;
        private long myCompletedTime = -1;
        private RepairOptions.RepairType myRepairType = RepairOptions.RepairType.VNODE;

        /**
         * Ongoing job build with ongoing job info.
         *
         * @param theJobId The job id.
         * @param theTokenMapHash Token map hash.
         * @param theRepairedTokens Repaired tokens.
         * @param theStatus Status.
         * @param theCompletedTime Completion time.
         * @return The builder
         */
        public Builder withOngoingJobInfo(final UUID theJobId,
                                          final int theTokenMapHash,
                                          final Set<UdtValue> theRepairedTokens,
                                          final Status theStatus,
                                          final Long theCompletedTime, final RepairOptions.RepairType repairType)
        {
            this.myJobId = theJobId;
            this.myTokenMapHash = theTokenMapHash;
            this.myRepairedTokens = theRepairedTokens;
            this.myStatus = theStatus;
            if (theCompletedTime != null)
            {
                this.myCompletedTime = theCompletedTime;
            }
            if (repairType != null)
            {
                this.myRepairType = repairType;
            }
            return this;
        }

        /**
         * Ongoing job build with table reference.
         *
         * @param aTableReference Table reference.
         * @return The builder
         */
        public Builder withTableReference(final TableReference aTableReference)
        {
            this.myTableReference = aTableReference;
            return this;
        }

        /**
         * Ongoing job build with on demand status.
         *
         * @param theOnDemandStatus Status.
         * @return The builder
         */
        public Builder withOnDemandStatus(final OnDemandStatus theOnDemandStatus)
        {
            this.myOnDemandStatus = theOnDemandStatus;
            return this;
        }

        /**
         * Ongoing job build with replication state.
         *
         * @param aReplicationState Replication state.
         * @return The builder
         */
        public Builder withReplicationState(final ReplicationState aReplicationState)
        {
            this.myReplicationState = aReplicationState;
            return this;
        }

        /**
         * Ongoing job build with host ID.
         *
         * @param aHostId Host id.
         * @return The builder
         */
        public Builder withHostId(final UUID aHostId)
        {
            this.myHostId = aHostId;
            return this;
        }

        /**
         * Ongoing job with repairType.
         *
         * @param repairType The repair type.
         * @return The builder
         */
        public Builder withRepairType(final RepairOptions.RepairType repairType)
        {
            this.myRepairType = repairType;
            return this;
        }

        /**
         * Ongoing job build.
         *
         * @return The job
         */
        public OngoingJob build()
        {
            return new OngoingJob(this);
        }
    }
}
