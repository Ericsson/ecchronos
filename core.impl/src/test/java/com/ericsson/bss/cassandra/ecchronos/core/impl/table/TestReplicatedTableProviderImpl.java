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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Verifies the cached, event-invalidated behaviour of {@link ReplicatedTableProviderImpl} (issue #1944): steady-state
 * {@code getAll()} does not recompute, {@code invalidate()} forces a refresh, and a keyspace not replicated to the
 * local DC is excluded without a per-call log flood.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class TestReplicatedTableProviderImpl
{
    private static final String LOCAL_DC = "dc1";
    private static final String REMOTE_DC = "dc2";

    @Mock
    private CqlSession mySession;
    @Mock
    private Metadata myMetadata;
    @Mock
    private TableReferenceFactory myTableReferenceFactory;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node myNode;

    private ReplicatedTableProviderImpl myProvider;

    @Before
    public void setup()
    {
        UUID nodeId = UUID.randomUUID();
        when(myNode.getHostId()).thenReturn(nodeId);
        when(myNode.getDatacenter()).thenReturn(LOCAL_DC);
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(nodeId, myNode));
        when(mySession.getMetadata()).thenReturn(myMetadata);
        lenient().when(myTableReferenceFactory.forTable(any(TableMetadata.class)))
                .thenAnswer(i -> mock(TableReference.class));
        myProvider = new ReplicatedTableProviderImpl(mySession, myTableReferenceFactory, myNativeConnectionProvider);
    }

    @Test
    public void testGetAllIsCachedAcrossCallsWithoutSchemaChange()
    {
        withKeyspaces(localReplicatedKeyspace("ks1", 1));

        Set<TableReference> first = myProvider.getAll();
        myProvider.getAll();
        myProvider.getAll();

        assertThat(first).hasSize(1);
        // The metadata (node x keyspace) scan happens only on the first call; later calls return the cached snapshot.
        verify(myNativeConnectionProvider, times(1)).getNodes();
    }

    @Test
    public void testInvalidateForcesRecompute()
    {
        withKeyspaces(localReplicatedKeyspace("ks1", 1));
        assertThat(myProvider.getAll()).hasSize(1);

        // Schema change: a second replicated keyspace appears. Without invalidate the cached set would be stale.
        withKeyspaces(localReplicatedKeyspace("ks1", 1), localReplicatedKeyspace("ks2", 1));
        assertThat(myProvider.getAll()).as("stale cache before invalidate").hasSize(1);

        myProvider.invalidate();
        assertThat(myProvider.getAll()).as("refreshed after invalidate").hasSize(2);
    }

    @Test
    public void testInvalidateDuringComputeIsNotMaskedByStalePublish()
    {
        // Simulate a schema change landing WHILE a recompute is in flight: the first getAll()'s compute reads the
        // driver metadata and, during that read, an invalidate() occurs (bumping the generation). The just-computed
        // (now potentially stale) snapshot must NOT be cached, so a later getAll() recomputes rather than serving a
        // generation-stale value. See issue #1944 (lost-invalidation race).
        withKeyspaces(localReplicatedKeyspace("ks1", 1));

        // getMetadata() is called during computeReplicatedTables(); fire invalidate() on the first such call only.
        AtomicBoolean invalidatedOnce = new AtomicBoolean(false);
        when(mySession.getMetadata()).thenAnswer(i ->
        {
            if (invalidatedOnce.compareAndSet(false, true))
            {
                myProvider.invalidate();
            }
            return myMetadata;
        });

        // First call computes but, because the generation moved mid-compute, must not publish to the cache.
        myProvider.getAll();
        long nodesCallsAfterFirst = mockingDetails(myNativeConnectionProvider).getInvocations().stream()
                .filter(inv -> "getNodes".equals(inv.getMethod().getName())).count();

        // A subsequent call must recompute (cache was not populated), i.e. getNodes() is called again.
        myProvider.getAll();
        long nodesCallsAfterSecond = mockingDetails(myNativeConnectionProvider).getInvocations().stream()
                .filter(inv -> "getNodes".equals(inv.getMethod().getName())).count();

        assertThat(nodesCallsAfterSecond)
                .as("second getAll recomputes because the mid-compute invalidate was not masked")
                .isGreaterThan(nodesCallsAfterFirst);
    }

    @Test
    public void testKeyspaceNotReplicatedToLocalDcIsExcludedAndNotReScanned()
    {
        // One keyspace replicated to the local DC, one only to a remote DC (must be excluded).
        withKeyspaces(localReplicatedKeyspace("ks_local", 1), remoteOnlyKeyspace("ks_remote"));

        Set<TableReference> tables = myProvider.getAll();
        myProvider.getAll();
        myProvider.getAll();

        assertThat(tables).as("only the locally-replicated keyspace's table is accepted").hasSize(1);
        // The exclusion decision is computed once and cached; no per-call re-scan.
        verify(myNativeConnectionProvider, times(1)).getNodes();
    }

    @Test
    public void testDefaultInterfaceInvalidateIsNoOp()
    {
        // A provider with no override (the interface default) must tolerate invalidate().
        com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider plain =
                new com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider()
                {
                    @Override
                    public Set<TableReference> getAll()
                    {
                        return Set.of();
                    }

                    @Override
                    public boolean accept(final Node node, final String keyspace)
                    {
                        return false;
                    }
                };
        plain.invalidate(); // must not throw
        assertThat(plain.getAll()).isEmpty();
    }

    private void withKeyspaces(final KeyspaceMetadata... keyspaces)
    {
        Map<CqlIdentifier, KeyspaceMetadata> map = new LinkedHashMap<>();
        for (KeyspaceMetadata ks : keyspaces)
        {
            map.put(ks.getName(), ks);
            // Metadata.getKeyspace(session, name) calls metadata.getKeyspace(String) (quoteIfNeeded leaves a simple
            // lowercase name unquoted), so stub the String overload with the plain name.
            when(myMetadata.getKeyspace(ks.getName().asInternal())).thenReturn(Optional.of(ks));
        }
        when(myMetadata.getKeyspaces()).thenReturn(map);
    }

    private KeyspaceMetadata localReplicatedKeyspace(final String name, final int tableCount)
    {
        return keyspace(name, Map.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                LOCAL_DC, "2"), tableCount);
    }

    private KeyspaceMetadata remoteOnlyKeyspace(final String name)
    {
        return keyspace(name, Map.of("class", "org.apache.cassandra.locator.NetworkTopologyStrategy",
                REMOTE_DC, "2"), 1);
    }

    private KeyspaceMetadata keyspace(final String name, final Map<String, String> replication, final int tableCount)
    {
        KeyspaceMetadata ks = mock(KeyspaceMetadata.class);
        CqlIdentifier id = CqlIdentifier.fromInternal(name);
        when(ks.getName()).thenReturn(id);
        lenient().when(ks.getReplication()).thenReturn(replication);
        Map<CqlIdentifier, TableMetadata> tables = new LinkedHashMap<>();
        for (int i = 0; i < tableCount; i++)
        {
            TableMetadata table = mock(TableMetadata.class);
            tables.put(CqlIdentifier.fromInternal(name + "_t" + i), table);
        }
        lenient().when(ks.getTables()).thenReturn(tables);
        return ks;
    }
}
