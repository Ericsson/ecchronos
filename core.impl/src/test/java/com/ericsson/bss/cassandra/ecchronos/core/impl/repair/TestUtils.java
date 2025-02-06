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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode.VnodeRepairStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.util.Preconditions;
import org.mockito.internal.util.collections.Sets;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;


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
                .withParallelism(RepairParallelism.PARALLEL)
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
        private RepairType repairType = RepairType.VNODE;

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

        public ScheduledRepairJobBuilder withRepairType(RepairType repairType)
        {
            this.repairType = repairType;
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
            return new ScheduledRepairJobView(id, id, tableReference(keyspace, table), repairConfiguration,
                    generateRepairStateSnapshot(lastRepairedAt, vnodeRepairStates), status,progress, lastRepairedAt + repairInterval, repairType);
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

    public static String getFailedRepairMessage(LongTokenRange... ranges)
    {
        Collection<LongTokenRange> rangeCollection = Arrays.asList(ranges);
        return String.format("Repair session RepairSession for range %s failed with error ...", rangeCollection);
    }

    public static String getRepairMessage(LongTokenRange... ranges)
    {
        Collection<LongTokenRange> rangeCollection = Arrays.asList(ranges);
        return String.format("Repair session RepairSession for range %s finished", rangeCollection);
    }

    public static Map<String, Integer> getNotificationData(int type, int progressCount, int total)
    {
        Map<String, Integer> data = new HashMap<>();
        data.put("type", type);
        data.put("progressCount", progressCount);
        data.put("total", total);
        return data;
    }
}

