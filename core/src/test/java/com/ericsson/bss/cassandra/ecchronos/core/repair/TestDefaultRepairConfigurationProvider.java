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

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.*;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;

@RunWith(MockitoJUnitRunner.class)
public class TestDefaultRepairConfigurationProvider
{
    private static final String KEYSPACE_NAME = "keyspace";
    private static final String TABLE_NAME = "table";

    private static final TableReference TABLE_REFERENCE = tableReference(KEYSPACE_NAME, TABLE_NAME);

    @Mock
    private Session session;

    @Mock
    private Cluster cluster;

    @Mock
    private Metadata metadata;

    @Mock
    private Host localhost;

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
            public Session getSession()
            {
                return session;
            }

            @Override
            public Host getLocalHost()
            {
                return localhost;
            }
        };

        when(session.getCluster()).thenReturn(cluster);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(cluster.register(any(SchemaChangeListener.class))).thenReturn(cluster);

        when(metadata.checkSchemaAgreement()).thenReturn(true);
        when(metadata.getKeyspaces()).thenReturn(Collections.emptyList());

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

        defaultRepairConfigurationProvider.onTableRemoved(tableMetadata);
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

        defaultRepairConfigurationProvider.onTableRemoved(tableMetadata);
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

        defaultRepairConfigurationProvider.onTableAdded(tableMetadata);

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

        defaultRepairConfigurationProvider.onTableAdded(tableMetadata);

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        defaultRepairConfigurationProvider.onTableRemoved(tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    @Test
    public void testAddNewNonReplicatedTable()
    {
        DefaultRepairConfigurationProvider defaultRepairConfigurationProvider = defaultRepairConfigurationProviderBuilder()
                .build();
        TableMetadata tableMetadata = mockNonReplicatedTable(TABLE_REFERENCE);

        defaultRepairConfigurationProvider.onTableAdded(tableMetadata);

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

        defaultRepairConfigurationProvider.onTableAdded(tableMetadata);

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        // Keyspace is no longer replicated
        when(myReplicatedTableProviderMock.accept(eq(TABLE_REFERENCE.getKeyspace()))).thenReturn(false);
        KeyspaceMetadata keyspaceMetadata = myKeyspaces.get(KEYSPACE_NAME);
        defaultRepairConfigurationProvider.onKeyspaceChanged(keyspaceMetadata, keyspaceMetadata);

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

        defaultRepairConfigurationProvider.onTableAdded(tableMetadata);

        verify(myRepairScheduler, never()).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));

        // Keyspace is now replicated
        when(myReplicatedTableProviderMock.accept(eq(TABLE_REFERENCE.getKeyspace()))).thenReturn(true);
        KeyspaceMetadata keyspaceMetadata = myKeyspaces.get(KEYSPACE_NAME);
        defaultRepairConfigurationProvider.onKeyspaceChanged(keyspaceMetadata, keyspaceMetadata);

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
                .withRepairConfiguration(tb -> {
                    if (tb.equals(tableReference2))
                    {
                        return customConfig;
                    }

                    return RepairConfiguration.DEFAULT;
                })
                .build();

        verify(myRepairScheduler).putConfiguration(eq(TABLE_REFERENCE), eq(RepairConfiguration.DEFAULT));
        verify(myRepairScheduler).putConfiguration(eq(tableReference2), eq(customConfig));

        defaultRepairConfigurationProvider.onTableRemoved(tableMetadata);
        verify(myRepairScheduler).removeConfiguration(eq(TABLE_REFERENCE));

        defaultRepairConfigurationProvider.onTableRemoved(tableMetadata2);
        verify(myRepairScheduler).removeConfiguration(eq(tableReference2));

        verifyNoMoreInteractions(myRepairScheduler);
        defaultRepairConfigurationProvider.close();
    }

    private DefaultRepairConfigurationProvider.Builder defaultRepairConfigurationProviderBuilder()
    {
        return DefaultRepairConfigurationProvider.newBuilder()
                .withReplicatedTableProvider(myReplicatedTableProviderMock)
                .withCluster(myNativeConnectionProvider.getSession().getCluster())
                .withDefaultRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairScheduler(myRepairScheduler)
                .withTableReferenceFactory(myTableReferenceFactory);
    }

    private TableMetadata mockNonReplicatedTable(TableReference tableReference)
    {
        return mockTable(tableReference, false);
    }

    private TableMetadata mockReplicatedTable(TableReference tableReference)
    {
        return mockTable(tableReference, true);
    }

    private TableMetadata mockTable(TableReference tableReference, boolean replicated)
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        myKeyspaces.put(tableReference.getKeyspace(), keyspaceMetadata);

        TableMetadata tableMetadata = mock(TableMetadata.class);

        when(tableMetadata.getKeyspace()).thenReturn(keyspaceMetadata);
        when(tableMetadata.getName()).thenReturn(tableReference.getTable());
        when(tableMetadata.getId()).thenReturn(UUID.randomUUID());

        when(keyspaceMetadata.getName()).thenReturn(tableReference.getKeyspace());
        when(keyspaceMetadata.getTables()).thenReturn(Collections.singletonList(tableMetadata));

        when(myReplicatedTableProviderMock.accept(eq(tableReference.getKeyspace()))).thenReturn(replicated);
        when(metadata.getKeyspaces()).thenReturn(new ArrayList<>(myKeyspaces.values()));
        when(metadata.getKeyspace(eq(tableReference.getKeyspace()))).thenReturn(keyspaceMetadata);

        return tableMetadata;
    }
}
