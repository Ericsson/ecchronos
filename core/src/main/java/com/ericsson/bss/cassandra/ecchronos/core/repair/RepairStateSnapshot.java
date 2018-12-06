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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * An immutable copy of the repair state.
 */
public class RepairStateSnapshot
{
    private final boolean canRepair;
    private final long myLastRepairedAt;
    private final ImmutableSet<Host> myReplicas;
    private final ImmutableSet<LongTokenRange> myLocalRangesForRepair;
    private final ImmutableSet<LongTokenRange> myRanges;
    private final ImmutableMap<LongTokenRange, Collection<Host>> myRangeToReplicas;
    private final ImmutableSet<String> myDataCenters;

    private RepairStateSnapshot(Builder builder)
    {
        canRepair = builder.canRepair;
        myLastRepairedAt = builder.myLastRepairedAt;
        myReplicas = builder.myReplicas;
        myLocalRangesForRepair = builder.myLocalRangesForRepair;
        myRanges = builder.myRanges;
        myRangeToReplicas = builder.myRangeToReplicas;
        myDataCenters = builder.myDataCenters;
    }

    /**
     * Check if a repair can be performed based on the current state.
     *
     * @return True if repair can run.
     */
    public boolean canRepair()
    {
        return canRepair;
    }

    /**
     * Get the time of the last successful repair of the table.
     *
     * @return The time the table was last repaired or -1 if no information is available.
     */
    public long lastRepairedAt()
    {
        return myLastRepairedAt;
    }

    /**
     * Get the replicas that should be part of the repair.
     *
     * This method will return an empty Set if all hosts should be part of the repair.
     *
     * @return The set of replicas that should be part of the repair.
     */
    public Set<Host> getReplicas()
    {
        return myReplicas;
    }

    /**
     * Get the ranges that needs to be repaired for the table.
     *
     * @return The ranges that should be repaired.
     */
    public Collection<LongTokenRange> getLocalRangesForRepair()
    {
        return myLocalRangesForRepair;
    }

    /**
     * Get all the local ranges for the node.
     *
     * @return The local ranges for the node.
     */
    public Collection<LongTokenRange> getAllRanges()
    {
        return myRanges;
    }

    /**
     * Get a map of the ranges and hosts associated to those ranges that needs to be repaired.
     *
     * @return The ranges in combination with the hosts to repair.
     */
    public Map<LongTokenRange, Collection<Host>> getRangeToReplicas()
    {
        return myRangeToReplicas;
    }

    /**
     * Get a collection of all data centers that should be part of the repair.
     *
     * @return The collection of data centers.
     */
    public Collection<String> getDatacentersForRepair()
    {
        return myDataCenters;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Boolean canRepair;
        private Long myLastRepairedAt;
        private ImmutableSet<Host> myReplicas;
        private ImmutableSet<LongTokenRange> myLocalRangesForRepair;
        private ImmutableSet<LongTokenRange> myRanges;
        private ImmutableMap<LongTokenRange, Collection<Host>> myRangeToReplicas;
        private ImmutableSet<String> myDataCenters;

        public Builder canRepair(boolean canRepair)
        {
            this.canRepair = canRepair;
            return this;
        }

        public Builder withLastRepairedAt(long lastRepairedAt)
        {
            myLastRepairedAt = lastRepairedAt;
            return this;
        }

        public Builder withReplicas(Collection<Host> replicas)
        {
            myReplicas = ImmutableSet.copyOf(replicas);
            return this;
        }

        public Builder withLocalRangesForRepair(Collection<LongTokenRange> localRangesForRepair)
        {
            myLocalRangesForRepair = ImmutableSet.copyOf(localRangesForRepair);
            return this;
        }

        public Builder withRanges(Collection<LongTokenRange> ranges)
        {
            myRanges = ImmutableSet.copyOf(ranges);
            return this;
        }

        public Builder withRangeToReplica(Map<LongTokenRange, Collection<Host>> rangeToReplica)
        {
            myRangeToReplicas = ImmutableMap.copyOf(rangeToReplica);
            return this;
        }

        public Builder withDataCenters(Collection<String> dataCenters)
        {
            myDataCenters = ImmutableSet.copyOf(dataCenters);
            return this;
        }

        public RepairStateSnapshot build()
        {
            return new RepairStateSnapshot(this);
        }
    }
}
