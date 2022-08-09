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

    private OngoingJob(Builder builder)
    {
        myOnDemandStatus = builder.onDemandStatus;
        myJobId = builder.jobId == null ? UUID.randomUUID() : builder.jobId;
        myHostId = builder.hostId;
        myTableReference = builder.tableReference;
        myReplicationState = builder.replicationState;
        myTokens = myReplicationState.getTokenRangeToReplicas(myTableReference);
        myRepairedTokens = builder.repairedTokens;
        myTokenHash = builder.tokenMapHash;
        myStatus = builder.status;
        myCompletedTime = builder.completedTime;

        if(myTokenHash == null)
        {
            myOnDemandStatus.addNewJob(myJobId, myTableReference, myTokens.keySet().hashCode());
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

    public Set<LongTokenRange> getRepairedTokens()
    {
        Set<LongTokenRange> repairedLongTokenRanges = new HashSet<>();
        myRepairedTokens.forEach(t -> repairedLongTokenRanges.add(new LongTokenRange(myOnDemandStatus.getStartTokenFrom(t), myOnDemandStatus.getEndTokenFrom(t))));
        return repairedLongTokenRanges;
    }

    public void finishRanges(Set<LongTokenRange> ranges)
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
                || (myTokenHash != null && (myTokenHash != myTokens.keySet().hashCode() && myTokenHash != myTokens.hashCode()));
    }

    public void startClusterWideJob()
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> allTokenRanges = myReplicationState.getTokenRanges(myTableReference);
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
            myOnDemandStatus.addNewJob(node.getId(), myJobId, myTableReference, allTokensForNode.hashCode(), repairedRanges);
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
        private UUID jobId = null;
        private UUID hostId;
        private TableReference tableReference;
        private Set<UdtValue> repairedTokens = new HashSet<>();
        private OnDemandStatus onDemandStatus;
        private ReplicationState replicationState;
        private Integer tokenMapHash = null;
        private Status status = Status.started;
        private long completedTime = -1;

        public Builder withOngoingJobInfo(UUID jobId, int tokenMapHash, Set<UdtValue> repairedTokens, Status status, Long completedTime)
        {
            this.jobId = jobId;
            this.tokenMapHash  = tokenMapHash;
            this.repairedTokens = repairedTokens;
            this.status = status;
            if(completedTime != null)
            {
                this.completedTime = completedTime;
            }
            return this;
        }

        public Builder withTableReference(TableReference tableReference)
        {
        	this.tableReference = tableReference;
        	return this;
        }

        public Builder withOnDemandStatus(OnDemandStatus onDemandStatus)
        {
            this.onDemandStatus = onDemandStatus;
        	return this;
        }

        public Builder withReplicationState(ReplicationState replicationState)
        {
            this.replicationState = replicationState;
            return this;
        }

        public Builder withHostId(UUID hostId)
        {
            this.hostId = hostId;
            return this;
        }

        public OngoingJob build()
        {
            return new OngoingJob(this);
        }
    }
}
