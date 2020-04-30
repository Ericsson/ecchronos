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
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
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

    public static RepairJobView createRepairJob(String keyspace, String table, long lastRepairedAt, long repairInterval)
    {
        return createRepairJob(UUID.randomUUID(), keyspace, table, lastRepairedAt, repairInterval, new LongTokenRange(1, 2), ImmutableSet.of());
    }

    public static RepairJobView createRepairJob(UUID id, String keyspace, String table, long lastRepairedAt, long repairInterval)
    {
        return createRepairJob(id, keyspace, table, lastRepairedAt, repairInterval, new LongTokenRange(1, 2), ImmutableSet.of());
    }

    public static RepairJobView createRepairJob(UUID id, String keyspace, String table, long lastRepairedAt, long repairInterval, LongTokenRange longTokenRange, ImmutableSet<Host> replicas)
    {
        VnodeRepairState vnodeRepairState = createVnodeRepairState(longTokenRange, replicas, lastRepairedAt);

        return createRepairJob(id, keyspace, table, lastRepairedAt, repairInterval, Sets.newSet(vnodeRepairState));
    }

    public static RepairJobView createRepairJob(UUID id, String keyspace, String table, long lastRepairedAt, long repairInterval, Collection<VnodeRepairState> vnodeRepairStateSet)
    {
        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(vnodeRepairStateSet).build();

        return new RepairJobView(id, new TableReference(keyspace, table),
                generateRepairConfiguration(repairInterval),
                generateRepairStateSnapshot(lastRepairedAt, vnodeRepairStates));
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
