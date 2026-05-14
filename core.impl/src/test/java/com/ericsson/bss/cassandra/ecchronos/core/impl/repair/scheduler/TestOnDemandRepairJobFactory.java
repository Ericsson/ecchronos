/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.IncrementalOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OnDemandStatus;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.OngoingJob;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.VnodeOnDemandRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestOnDemandRepairJobFactory
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace1", "table1");

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private TableRepairMetrics myTableRepairMetrics;
    @Mock
    private ReplicationState myReplicationState;
    @Mock
    private RepairHistory myRepairHistory;
    @Mock
    private OnDemandStatus myOnDemandStatus;
    @Mock
    private Node myNode;
    @Mock
    private OngoingJob myOngoingJob;
    @Mock
    private BiConsumer<UUID, UUID> myOnFinishedHook;

    private OnDemandRepairJobFactory myFactory;
    private final UUID myNodeId = UUID.randomUUID();

    @Before
    public void setup()
    {
        Map<UUID, Node> nodeMap = new HashMap<>();
        nodeMap.put(myNodeId, myNode);
        when(myOnDemandStatus.getNodes()).thenReturn(nodeMap);
        when(myNode.getHostId()).thenReturn(myNodeId);
        when(myOngoingJob.getTableReference()).thenReturn(TABLE_REFERENCE);
        when(myOngoingJob.getHostId()).thenReturn(myNodeId);
        when(myOngoingJob.getJobId()).thenReturn(UUID.randomUUID());

        myFactory = OnDemandRepairJobFactory.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withReplicationState(myReplicationState)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairHistory(myRepairHistory)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withOnDemandStatus(myOnDemandStatus)
                .withOnFinishedHook(myOnFinishedHook)
                .build();
    }

    @Test
    public void testCreateVnodeJobFromOngoingJob()
    {
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.VNODE);

        OnDemandRepairJob job = myFactory.createFromOngoingJob(myOngoingJob);

        assertThat(job).isInstanceOf(VnodeOnDemandRepairJob.class);
        assertThat(job.getOngoingJob()).isEqualTo(myOngoingJob);
    }

    @Test
    public void testCreateIncrementalJobFromOngoingJob()
    {
        when(myOngoingJob.getRepairType()).thenReturn(RepairType.INCREMENTAL);
        when(myReplicationState.getReplicas(TABLE_REFERENCE, myNode)).thenReturn(ImmutableSet.of(mock(DriverNode.class)));

        OnDemandRepairJob job = myFactory.createFromOngoingJob(myOngoingJob);

        assertThat(job).isInstanceOf(IncrementalOnDemandRepairJob.class);
        assertThat(job.getOngoingJob()).isEqualTo(myOngoingJob);
    }
}
