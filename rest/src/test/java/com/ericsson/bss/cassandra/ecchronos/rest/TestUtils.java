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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
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

    public static RepairJobView createRepairJob(String keyspace, String table, long lastRepairedAt, long repairInterval)
    {
        return createRepairJob(keyspace, table, lastRepairedAt, repairInterval, new LongTokenRange(1, 2), Collections.emptySet());
    }

    public static RepairJobView createRepairJob(String keyspace, String table, long lastRepairedAt, long repairInterval, LongTokenRange longTokenRange, Set<Host> replicas)
    {
        VnodeRepairState vnodeRepairState = createVnodeRepairState(longTokenRange, replicas, lastRepairedAt);

        return createRepairJob(keyspace, table, lastRepairedAt, repairInterval, Sets.newSet(vnodeRepairState));
    }

    public static RepairJobView createRepairJob(String keyspace, String table, long lastRepairedAt, long repairInterval, Collection<VnodeRepairState> vnodeRepairStateSet)
    {
        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(vnodeRepairStateSet).build();

        return new RepairJobView(new TableReference(keyspace, table),
                generateRepairConfiguration(repairInterval),
                generateRepairStateSnapshot(lastRepairedAt, vnodeRepairStates));
    }

    public static VnodeRepairState createVnodeRepairState(long startToken, long endToken, Set<Host> replicas, long lastRepairedAt)
    {
        return createVnodeRepairState(new LongTokenRange(startToken, endToken), replicas, lastRepairedAt);
    }

    public static VnodeRepairState createVnodeRepairState(LongTokenRange longTokenRange, Set<Host> replicas, long lastRepairedAt)
    {
        return new VnodeRepairState(longTokenRange, replicas, lastRepairedAt);
    }
}
