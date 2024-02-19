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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestDefaultRepairConfigurationProvider
{
    private static final String KEYSPACE_NAME = "keyspace1";
    private static final String TABLE_NAME = "table1";

    private static final TableReference TABLE_REFERENCE = tableReference(KEYSPACE_NAME, TABLE_NAME);

    @Mock
    private CqlSession session;

    @Mock
    private Metadata metadata;

    @Mock
    private Node localNode;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProviderMock;

    @Mock
    private RepairScheduler myRepairScheduler;

    private TableReferenceFactory myTableReferenceFactory = new MockTableReferenceFactory();

    private NativeConnectionProvider myNativeConnectionProvider;

    private Map<String, KeyspaceMetadata> myKeyspaces = new HashMap<>();

    @Before
    public void setupMocks()
    {
        myNativeConnectionProvider = new NativeConnectionProvider()
        {
            @Override
            public CqlSession getSession()
            {
                return session;
            }

            @Override
            public Node getLocalNode()
            {
                return localNode;
            }

            @Override
            public boolean getRemoteRouting()
            {
                return true;
            }

            @Override
            public String getSerialConsistency(){
                return "DEFAULT";
            }
        };

        when(session.getMetadata()).thenReturn(metadata);
        when(metadata.getKeyspaces()).thenReturn(Collections.emptyMap());

        when(myReplicatedTableProviderMock.accept(anyString())).thenReturn(false);
    }

    @Test
    public void testExistingTablesAreScheduled()
    {
        // Create the table metadata before creating the repair configuration provider
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testNonReplicatedExistingTablesAreNotScheduled()
    {
        // Create the table metadata before creating the repair configuration provider
        TableMetadata tableMetadata = mockNonReplicatedTable(TABLE_REFERENCE);
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler, never()).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewTable()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        defaultRepairConfigurationProvider.close();
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
    }

    @Test
    public void testRemoveTable()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewNonReplicatedKeyspace()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        KeyspaceMetadata keyspaceMetadata = mockKeyspace(TABLE_REFERENCE.getKeyspace(), false);
        mockTable(keyspaceMetadata, TABLE_REFERENCE, new HashMap<>());

        defaultRepairConfigurationProvider.onKeyspaceCreated(keyspaceMetadata);

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairScheduler, times(1)).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewReplicatedKeyspace()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        KeyspaceMetadata keyspaceMetadata = mockKeyspace(TABLE_REFERENCE.getKeyspace(), true);
        mockTable(keyspaceMetadata, TABLE_REFERENCE, new HashMap<>());

        defaultRepairConfigurationProvider.onKeyspaceCreated(keyspaceMetadata);

        verify(myRepairScheduler, times(1)).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairScheduler, never()).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewNonReplicatedTable()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockNonReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairScheduler, never()).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewReplicatedTableChangedToNonReplicated()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        // Keyspace is no longer replicated
        when(myReplicatedTableProviderMock.accept(eq(TABLE_REFERENCE.getKeyspace()))).thenReturn(false);
        KeyspaceMetadata keyspaceMetadata = myKeyspaces.get(KEYSPACE_NAME);
        defaultRepairConfigurationProvider.onKeyspaceUpdated(keyspaceMetadata, keyspaceMetadata);

        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewNonReplicatedTableChangedToReplicated()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockNonReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        // Keyspace is now replicated
        when(myReplicatedTableProviderMock.accept(eq(TABLE_REFERENCE.getKeyspace()))).thenReturn(true);
        KeyspaceMetadata keyspaceMetadata = myKeyspaces.get(KEYSPACE_NAME);
        defaultRepairConfigurationProvider.onKeyspaceUpdated(keyspaceMetadata, keyspaceMetadata);

        verify(myRepairScheduler, never()).removeConfiguration(eq(TABLE_REFERENCE));
        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testMultipleTableSchedules()
    {
        // Create the table metadata before creating the repair configuration provider
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);

        TableReference tableReference2 = tableReference("keyspace2", TABLE_NAME);
        TableMetadata tableMetadata2 = mockReplicatedTable(tableReference2);

        RepairConfiguration customConfig = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .build();

        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .withRepairConfiguration(tb ->
                {
                    if (tb.equals(tableReference2))
                    {
                        return customConfig;
                    }

                    return RepairConfiguration.DEFAULT;
                })
                .build();

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairScheduler).putConfiguration(eq(tableReference2), eq(customConfig));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata2);
        verify(myRepairScheduler).removeConfiguration(eq(tableReference2));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testDisabledRepairConfiguration()
    {
        // Create the table metadata before creating the repair configuration provider
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE);
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .withRepairConfiguration(tb -> RepairConfiguration.DISABLED)
                .build();

        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler, times(2)).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testExistingTablesWithTWCSAreNotScheduled()
    {
        // Create the table metadata before creating the repair configuration provider
        Map<CqlIdentifier, Object> tableOptions = new HashMap<>();
        Map<String, String> compaction = new HashMap<>();
        compaction.put("class", "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy");
        tableOptions.put(CqlIdentifier.fromInternal("compaction"), compaction);
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE, tableOptions);
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder().withIgnoreTWCSTables(true).build();
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .withDefaultRepairConfiguration(repairConfiguration)
                .build();

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(repairConfiguration));

        defaultRepairConfigurationProvider.onTableDropped(tableMetadata);
        verify(myRepairScheduler, times(2)).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testTableChangesFromTWCS()
    {
        Map<CqlIdentifier, Object> tableOptions = new HashMap<>();
        Map<String, String> compaction = new HashMap<>();
        compaction.put("class", "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy");
        tableOptions.put(CqlIdentifier.fromInternal("compaction"), compaction);
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE, tableOptions);
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder().withIgnoreTWCSTables(true).build();
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .withDefaultRepairConfiguration(repairConfiguration)
                .build();

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);
        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(repairConfiguration));
        verify(myRepairScheduler, times(2)).removeConfiguration(eq(TABLE_REFERENCE));

        Map<String, String> updatedCompaction = new HashMap<>();
        updatedCompaction.put("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
        tableOptions.put(CqlIdentifier.fromInternal("compaction"), updatedCompaction);
        TableMetadata updatedTableMetadata = mockReplicatedTable(TABLE_REFERENCE, tableOptions);

        defaultRepairConfigurationProvider.onTableUpdated(updatedTableMetadata, tableMetadata);
        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(repairConfiguration));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testTableChangesToTWCS()
    {
        Map<CqlIdentifier, Object> tableOptions = new HashMap<>();
        Map<String, String> compaction = new HashMap<>();
        compaction.put("class", "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy");
        tableOptions.put(CqlIdentifier.fromInternal("compaction"), compaction);
        TableMetadata tableMetadata = mockReplicatedTable(TABLE_REFERENCE, tableOptions);
        RepairConfiguration repairConfiguration = RepairConfiguration.newBuilder().withIgnoreTWCSTables(true).build();
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .withDefaultRepairConfiguration(repairConfiguration)
                .build();

        defaultRepairConfigurationProvider.onTableCreated(tableMetadata);
        verify(myRepairScheduler, times(2)).putConfiguration(eq(TABLE_REFERENCE), eq(repairConfiguration));

        Map<String, String> updatedCompaction = new HashMap<>();
        updatedCompaction.put("class", "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy");
        tableOptions.put(CqlIdentifier.fromInternal("compaction"), updatedCompaction);
        TableMetadata updatedTableMetadata = mockReplicatedTable(TABLE_REFERENCE, tableOptions);

        defaultRepairConfigurationProvider.onTableUpdated(updatedTableMetadata, tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    private DefaultRepairConfigurationProvider.Builder defaultRepairConfigurationProviderBuilder()
    {
        return DefaultRepairConfigurationProvider.newBuilder()
                .withReplicatedTableProvider(myReplicatedTableProviderMock)
                .withSession(myNativeConnectionProvider.getSession())
                .withDefaultRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairScheduler(myRepairScheduler)
                .withTableReferenceFactory(myTableReferenceFactory);
    }

    private TableMetadata mockNonReplicatedTable(TableReference tableReference)
    {
        return mockNonReplicatedTable(tableReference, new HashMap<>());
    }

    private TableMetadata mockReplicatedTable(TableReference tableReference)
    {
        return mockReplicatedTable(tableReference, new HashMap<>());
    }

    private TableMetadata mockNonReplicatedTable(TableReference tableReference, Map<CqlIdentifier, Object> compactionOptions)
    {
        KeyspaceMetadata keyspaceMetadata = mockKeyspace(tableReference.getKeyspace(), false);
        return mockTable(keyspaceMetadata, tableReference, compactionOptions);
    }

    private TableMetadata mockReplicatedTable(TableReference tableReference, Map<CqlIdentifier, Object> compactionOptions)
    {
        KeyspaceMetadata keyspaceMetadata = mockKeyspace(tableReference.getKeyspace(), true);
        return mockTable(keyspaceMetadata, tableReference, compactionOptions);
    }

    private KeyspaceMetadata mockKeyspace(String name, boolean replicated)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(name));
        when(myReplicatedTableProviderMock.accept(eq(name))).thenReturn(replicated);
        return keyspaceMetadata;
    }

    private TableMetadata mockTable(KeyspaceMetadata keyspaceMetadata, TableReference tableReference,
            Map<CqlIdentifier, Object> compactionOptions)
    {
        myKeyspaces.put(tableReference.getKeyspace(), keyspaceMetadata);

        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(tableMetadata.getName()).thenReturn(CqlIdentifier.fromInternal(tableReference.getTable()));
        doReturn(keyspaceMetadata.getName()).when(tableMetadata).getKeyspace();
        doReturn(Optional.of(tableReference.getId())).when(tableMetadata).getId();
        doReturn(Collections.singletonMap(tableMetadata.getName(), tableMetadata)).when(keyspaceMetadata).getTables();
        Map<CqlIdentifier, Object> options = new HashMap<>();
        options.putAll(compactionOptions);
        options.put(CqlIdentifier.fromInternal("gc_grace_seconds"), MockTableReferenceFactory.DEFAULT_GC_GRACE_SECONDS);
        when(tableMetadata.getOptions()).thenReturn(options);

        Map<CqlIdentifier, KeyspaceMetadata> keyspaceMetadatas = new HashMap<>();
        for (Map.Entry<String, KeyspaceMetadata> keyspaceEntry : myKeyspaces.entrySet())
        {
            keyspaceMetadatas.put(CqlIdentifier.fromInternal(keyspaceEntry.getKey()), keyspaceEntry.getValue());
        }
        when(metadata.getKeyspaces()).thenReturn(keyspaceMetadatas);
        when(metadata.getKeyspace(eq(tableReference.getKeyspace()))).thenReturn(Optional.of(keyspaceMetadata));

        return tableMetadata;
    }
}
