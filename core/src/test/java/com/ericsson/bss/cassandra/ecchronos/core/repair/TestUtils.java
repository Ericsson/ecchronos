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

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Preconditions;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestUtils
{
    public static RepairStateSnapshot generateRepairStateSnapshot(long lastRepairedAt, VnodeRepairStates vnodeRepairStates)
    {
        return RepairStateSnapshot.newBuilder()
                .withLastRepairedAt(lastRepairedAt)
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

    public static class RepairJobBuilder
    {
        private UUID id = UUID.randomUUID();
        private String keyspace;
        private String table;
        private long lastRepairedAt = 0;
        private long repairInterval = 0;
        private ImmutableSet<Host> replicas = ImmutableSet.of();
        private LongTokenRange longTokenRange = new LongTokenRange(1, 2);
        private Collection<VnodeRepairState> vnodeRepairStateSet;

        private double progress = 0;
        private RepairJobView.Status status = RepairJobView.Status.IN_QUEUE;

        public RepairJobBuilder withId(UUID id)
        {
            this.id = id;
            return this;
        }

        public RepairJobBuilder withKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public RepairJobBuilder withTable(String table)
        {
            this.table = table;
            return this;
        }

        public RepairJobBuilder withLastRepairedAt(long lastRepairedAt)
        {
            this.lastRepairedAt = lastRepairedAt;
            return this;
        }

        public RepairJobBuilder withRepairInterval(long repairInterval)
        {
            this.repairInterval = repairInterval;
            return this;
        }


        public RepairJobBuilder withVnodeRepairStateSet(Collection<VnodeRepairState> vnodeRepairStateSet)
        {
            this.vnodeRepairStateSet = vnodeRepairStateSet;
            return this;
        }

        public RepairJobBuilder withStatus(RepairJobView.Status status)
        {
            this.status = status;
            return this;
        }

        public RepairJobBuilder withProgress(double progress)
        {
            this.progress = progress;
            return this;
        }

        public RepairJobView build()
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

            return new RepairJobView(id, new TableReference(keyspace, table),
                    generateRepairConfiguration(repairInterval),
                    generateRepairStateSnapshot(lastRepairedAt, vnodeRepairStates), status,progress);
        }
    }


    public static VnodeRepairState createVnodeRepairState(long startToken, long endToken, ImmutableSet<Host> replicas, long lastRepairedAt)
    {
        return createVnodeRepairState(new LongTokenRange(startToken, endToken), replicas, lastRepairedAt);
    }

    public static VnodeRepairState createVnodeRepairState(LongTokenRange longTokenRange, ImmutableSet<Host> replicas, long lastRepairedAt)
    {
        return new VnodeRepairState(longTokenRange, replicas, lastRepairedAt);
    }
}
