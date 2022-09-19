/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Preconditions;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;

public class TestUtils
{
    public static RepairStateSnapshot generateRepairStateSnapshot(long lastRepairedAt, VnodeRepairStates vnodeRepairStates)
    {
        return RepairStateSnapshot.newBuilder()
                .withLastCompletedAt(lastRepairedAt)
                .withVnodeRepairStates(vnodeRepairStates)
                .withReplicaRepairGroups(Collections.emptyList())
                .build();
    }

    public static RepairConfiguration generateRepairConfiguration(long repairIntervalInMs)
    {
        return RepairConfiguration.newBuilder().withRepairInterval(repairIntervalInMs, TimeUnit.MILLISECONDS).build();
    }

    public static RepairConfiguration createRepairConfiguration(long interval, double unwindRatio, int warningTime, int errorTime)
    {
        return RepairConfiguration.newBuilder()
                .withRepairInterval(interval, TimeUnit.MILLISECONDS)
                .withParallelism(RepairOptions.RepairParallelism.PARALLEL)
                .withRepairUnwindRatio(unwindRatio)
                .withRepairWarningTime(warningTime, TimeUnit.MILLISECONDS)
                .withRepairErrorTime(errorTime, TimeUnit.MILLISECONDS)
                .build();
    }

    public static class ScheduledRepairJobBuilder
    {
        private UUID id = UUID.randomUUID();
        private String keyspace;
        private String table;
        private long lastRepairedAt = 0;
        private long repairInterval = 0;
        private ImmutableSet<DriverNode> replicas = ImmutableSet.of();
        private LongTokenRange longTokenRange = new LongTokenRange(1, 2);
        private Collection<VnodeRepairState> vnodeRepairStateSet;
        private RepairConfiguration repairConfiguration;

        private double progress = 0;
        private ScheduledRepairJobView.Status status = ScheduledRepairJobView.Status.ON_TIME;

        public ScheduledRepairJobBuilder withId(UUID id)
        {
            this.id = id;
            return this;
        }

        public ScheduledRepairJobBuilder withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public ScheduledRepairJobBuilder withTable(String table)
        {
            this.table = table;
            return this;
        }

        public ScheduledRepairJobBuilder withLastRepairedAt(long lastRepairedAt)
        {
            this.lastRepairedAt = lastRepairedAt;
            return this;
        }

        public ScheduledRepairJobBuilder withRepairInterval(long repairInterval)
        {
            this.repairInterval = repairInterval;
            return this;
        }

        public ScheduledRepairJobBuilder withVnodeRepairStateSet(Collection<VnodeRepairState> vnodeRepairStateSet)
        {
            this.vnodeRepairStateSet = vnodeRepairStateSet;
            return this;
        }

        public ScheduledRepairJobBuilder withStatus(ScheduledRepairJobView.Status status)
        {
            this.status = status;
            return this;
        }

        public ScheduledRepairJobBuilder withProgress(double progress)
        {
            this.progress = progress;
            return this;
        }

        public ScheduledRepairJobBuilder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }


        public ScheduledRepairJobView build()
        {
            Preconditions.checkNotNull(keyspace, "Keyspace cannot be null");
            Preconditions.checkNotNull(table, "Table cannot be null");
            Preconditions.checkArgument(lastRepairedAt > 0, "Last repaired not set");
            Preconditions.checkArgument(repairInterval > 0, "Repair interval not set");
            VnodeRepairStates vnodeRepairStates;
            if ( vnodeRepairStateSet != null)
            {
                vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(vnodeRepairStateSet).build();
            }
            else
            {
                VnodeRepairState vnodeRepairState = createVnodeRepairState(longTokenRange, replicas, lastRepairedAt);
                vnodeRepairStates = VnodeRepairStatesImpl.newBuilder(Sets.newSet(vnodeRepairState)).build();
            }

            if (repairConfiguration == null)
            {
                this.repairConfiguration = generateRepairConfiguration(repairInterval);
            }
            return new ScheduledRepairJobView(id, tableReference(keyspace, table), repairConfiguration,
                    generateRepairStateSnapshot(lastRepairedAt, vnodeRepairStates), status,progress, lastRepairedAt + repairInterval);
        }
    }

    public static class OnDemandRepairJobBuilder
    {
        private UUID id = UUID.randomUUID();
        private UUID hostId = UUID.randomUUID();
        private String keyspace;
        private String table;
        private long completedAt = 0;

        private double progress = 0;
        private OnDemandRepairJobView.Status status = OnDemandRepairJobView.Status.IN_QUEUE;
		private RepairConfiguration repairConfiguration = RepairConfiguration.DEFAULT;

        public OnDemandRepairJobBuilder withId(UUID id)
        {
            this.id = id;
            return this;
        }

        public OnDemandRepairJobBuilder withHostId(UUID hostId)
        {
            this.hostId = hostId;
            return this;
        }

        public OnDemandRepairJobBuilder withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public OnDemandRepairJobBuilder withTable(String table)
        {
            this.table = table;
            return this;
        }

        public OnDemandRepairJobBuilder withCompletedAt(long completedAt)
        {
            this.completedAt = completedAt;
            return this;
        }

        public OnDemandRepairJobBuilder withRepairConfiguration(RepairConfiguration repairConfiguration)
        {
            this.repairConfiguration = repairConfiguration;
            return this;
        }

        public OnDemandRepairJobBuilder withStatus(OnDemandRepairJobView.Status status)
        {
            this.status = status;
            return this;
        }

        public OnDemandRepairJobBuilder withProgress(double progress)
        {
            this.progress = progress;
            return this;
        }

        public OnDemandRepairJobView build()
        {
            Preconditions.checkNotNull(keyspace, "Keyspace cannot be null");
            Preconditions.checkNotNull(table, "Table cannot be null");
            Preconditions.checkArgument(completedAt > 0 || completedAt == -1, "Last repaired not set");

            return new OnDemandRepairJobView(id, hostId, tableReference(keyspace, table), status, progress, completedAt);
        }
    }

    public static VnodeRepairState createVnodeRepairState(long startToken, long endToken, ImmutableSet<DriverNode> replicas,
            long lastRepairedAt)
    {
        return createVnodeRepairState(new LongTokenRange(startToken, endToken), replicas, lastRepairedAt);
    }

    public static VnodeRepairState createVnodeRepairState(LongTokenRange longTokenRange, ImmutableSet<DriverNode> replicas,
            long lastRepairedAt)
    {
        return new VnodeRepairState(longTokenRange, replicas, lastRepairedAt);
    }
}
