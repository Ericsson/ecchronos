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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.UDTValue;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

public class OngoingJob
{
    private final UUID myJobId;
    private final TableReference myTableReference;
    private final Map<LongTokenRange, ImmutableSet<Node>> myTokens;
    private final Set<UDTValue> myRepairedTokens;
    private final OnDemandStatus myOnDemandStatus;
	private final ReplicationState myReplicationState;
    private final Integer myTokenHash;

    private OngoingJob(Builder builder)
    {
        myOnDemandStatus = builder.onDemandStatus;
        myJobId = builder.jobId == null ? UUID.randomUUID() : builder.jobId;
        myTableReference = builder.tableReference;
        myReplicationState = builder.replicationState;
        myTokens = myReplicationState.getTokenRangeToReplicas(myTableReference);
        myRepairedTokens = builder.repairedTokens;
        myTokenHash = builder.tokenMapHash;

        if(myTokenHash == null)
        {
            myOnDemandStatus.addNewJob(myJobId, myTableReference, myTokens.hashCode());
        }
    }

    public UUID getJobId()
    {
        return myJobId;
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

    public Map<LongTokenRange, ImmutableSet<Node>> getTokens()
    {
    	return myTokens;
    }

    public boolean hasTopologyChanged()
    {
    	return !myTokens.equals(myReplicationState.getTokenRangeToReplicas(myTableReference))
                || (myTokenHash != null && myTokenHash != myTokens.hashCode());
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
        private TableReference tableReference;
        private Set<UDTValue> repairedTokens = new HashSet<>();
		private OnDemandStatus onDemandStatus;
		private ReplicationState replicationState;
		private Integer tokenMapHash = null;

        public Builder withOngoingJobInfo(UUID jobId, int tokenMapHash, Set<UDTValue> repairedTokens)
        {
        	this.jobId = jobId;
        	this.tokenMapHash  = tokenMapHash;
        	this.repairedTokens = repairedTokens;
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

        public OngoingJob build()
        {
            return new OngoingJob(this);
        }
    }
}
