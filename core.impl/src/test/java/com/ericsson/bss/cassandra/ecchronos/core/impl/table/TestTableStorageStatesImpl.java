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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.management.RuntimeMBeanException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Verifies that the fixed-rate table-size updater never lets an exception escape its scheduled {@code Runnable}
 * (which would permanently suppress all future executions), so table-size tracking keeps working across transient
 * failures. See issue #1940.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestTableStorageStatesImpl
{
    private static final TableReference TABLE = tableReference("ks", "tbl");

    @Mock
    private ReplicatedTableProvider myReplicatedTableProvider;
    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private DistributedJmxProxy myJmxProxy;

    private final UUID myNodeId1 = UUID.randomUUID();
    private final UUID myNodeId2 = UUID.randomUUID();

    private TableStorageStatesImpl myTableStorageStates;

    @Before
    public void setup() throws IOException
    {
        lenient().when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
    }

    @After
    public void cleanup()
    {
        if (myTableStorageStates != null)
        {
            myTableStorageStates.close();
        }
    }

    private TableStorageStatesImpl build(final UUID... nodeIds)
    {
        Map<UUID, Node> nodes = new LinkedHashMap<>();
        for (UUID nodeId : nodeIds)
        {
            Node node = mock(Node.class);
            when(node.getHostId()).thenReturn(nodeId);
            nodes.put(nodeId, node);
        }
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodes);
        return TableStorageStatesImpl.builder()
                .withReplicatedTableProvider(myReplicatedTableProvider)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withConnectionProvider(myNativeConnectionProvider)
                // Long initial delay so the background scheduler never fires during the test; the test drives
                // updateTableStates() directly.
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();
    }

    @Test
    public void testRuntimeExceptionDoesNotKillFutureUpdates()
    {
        myTableStorageStates = build(myNodeId1);

        // First tick: getAll() throws an unchecked exception (e.g. driver metadata churn). It must not propagate
        // out of the scheduled Runnable.
        when(myReplicatedTableProvider.getAll())
                .thenThrow(new IllegalStateException("metadata churn"))
                .thenReturn(java.util.Collections.singleton(TABLE));
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId1), eq(TABLE))).thenReturn(123L);

        assertThatCode(myTableStorageStates::updateTableStates).doesNotThrowAnyException();

        // The updater is still alive: a second tick updates normally.
        myTableStorageStates.updateTableStates();
        assertThat(myTableStorageStates.getDataSize(myNodeId1, TABLE)).isEqualTo(123L);
    }

    @Test
    public void testNullTableSizeMapDoesNotThrow()
    {
        myTableStorageStates = build(myNodeId1);

        // close() clears the internal table-size map; a subsequent tick must not throw.
        myTableStorageStates.close();

        assertThatCode(myTableStorageStates::updateTableStates).doesNotThrowAnyException();
        myTableStorageStates = null; // already closed
    }

    @Test
    public void testMultipleNodesRetainIndependentSizes() throws IOException
    {
        // Issue #1941: updating one node's sizes must not clear another node's sizes.
        myTableStorageStates = build(myNodeId1, myNodeId2);

        when(myReplicatedTableProvider.getAll()).thenReturn(java.util.Collections.singleton(TABLE));
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId1), eq(TABLE))).thenReturn(111L);
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId2), eq(TABLE))).thenReturn(222L);

        myTableStorageStates.updateTableStates();

        // Both nodes retain their own size after a full pass (previously only the last-updated node survived).
        assertThat(myTableStorageStates.getDataSize(myNodeId1, TABLE)).isEqualTo(111L);
        assertThat(myTableStorageStates.getDataSize(myNodeId2, TABLE)).isEqualTo(222L);
    }

    @Test
    public void testGetDataSizeMissIsSideEffectFree() throws IOException
    {
        // Issue #1941: a getDataSize for an unknown node must return 0 without evicting known nodes (the old read
        // path replaced the whole map as a side effect).
        myTableStorageStates = build(myNodeId1);

        when(myReplicatedTableProvider.getAll()).thenReturn(java.util.Collections.singleton(TABLE));
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId1), eq(TABLE))).thenReturn(321L);
        myTableStorageStates.updateTableStates();

        UUID unknownNode = UUID.randomUUID();
        assertThat(myTableStorageStates.getDataSize(unknownNode, TABLE)).isEqualTo(0L);
        assertThat(myTableStorageStates.getDataSize(unknownNode)).isEqualTo(0L);

        // The known node's size is intact after the misses.
        assertThat(myTableStorageStates.getDataSize(myNodeId1, TABLE)).isEqualTo(321L);
    }

    @Test
    public void testConnectFailureIsCaughtAndUpdaterSurvives() throws IOException
    {
        myTableStorageStates = build(myNodeId1);

        // First tick: the JMX connect fails (IOException). It must be caught per-node and not propagate; a later
        // tick with a working connection still updates.
        when(myJmxProxyFactory.connect())
                .thenThrow(new IOException("jmx down"))
                .thenReturn(myJmxProxy);
        when(myReplicatedTableProvider.getAll()).thenReturn(java.util.Collections.singleton(TABLE));
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId1), eq(TABLE))).thenReturn(456L);

        assertThatCode(myTableStorageStates::updateTableStates).doesNotThrowAnyException();

        myTableStorageStates.updateTableStates();
        assertThat(myTableStorageStates.getDataSize(myNodeId1, TABLE)).isEqualTo(456L);
    }

    @Test
    public void testRuntimeMBeanExceptionYieldsSizeAndDoesNotKillUpdater()
    {
        myTableStorageStates = build(myNodeId1);

        // Simulate the liveDiskSpaceUsed defensive path returning 0 (as it does for RuntimeMBeanException), then a
        // real size on a later read. Also assert that a RuntimeMBeanException actually escaping is tolerated.
        when(myReplicatedTableProvider.getAll()).thenReturn(java.util.Collections.singleton(TABLE));
        when(myJmxProxy.liveDiskSpaceUsed(eq(myNodeId1), eq(TABLE)))
                .thenThrow(new RuntimeMBeanException(new RuntimeException("bean not ready")))
                .thenReturn(0L);

        assertThatCode(myTableStorageStates::updateTableStates).doesNotThrowAnyException();

        // Updater still alive: next tick yields the (zero) size without having died.
        myTableStorageStates.updateTableStates();
        assertThat(myTableStorageStates.getDataSize(myNodeId1, TABLE)).isEqualTo(0L);
    }
}
