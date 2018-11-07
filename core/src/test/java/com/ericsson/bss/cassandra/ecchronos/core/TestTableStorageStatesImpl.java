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
package com.ericsson.bss.cassandra.ecchronos.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestTableStorageStatesImpl
{
    @Mock
    private JmxProxyFactory myJmxProxyFactory;

    @Mock
    private JmxProxy myJmxProxy;

    @Mock
    private ReplicatedTableProvider myReplicatedTableProviderMock;

    private Set<TableReference> myReplicatedTables = new HashSet<>();

    private TableStorageStatesImpl myTableStorageeStatesImpl;

    @Before
    public void init() throws Exception
    {
        doReturn(myJmxProxy).when(myJmxProxyFactory).connect();
        doReturn(myReplicatedTables).when(myReplicatedTableProviderMock).getAll();

        myTableStorageeStatesImpl = TableStorageStatesImpl.builder()
                .withReplicatedTableProvider(myReplicatedTableProviderMock)
                .withJmxProxyFactory(myJmxProxyFactory)
                .withInitialDelay(60, TimeUnit.SECONDS)
                .build();
    }

    @After
    public void close()
    {
        myTableStorageeStatesImpl.close();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testBuildWithNullReplicatedTableProvider()
    {
        TableStorageStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .withReplicatedTableProvider(null)
                .build();
    }

    @Test (expected = IllegalArgumentException.class)
    public void testBuildWithNullJmxProxyFactory()
    {
        TableStorageStatesImpl.builder()
                .withJmxProxyFactory(null)
                .withReplicatedTableProvider(myReplicatedTableProviderMock)
                .build();
    }

    @Test
    public void testTableStatesForNoTables()
    {
        assertThat(myTableStorageeStatesImpl.getDataSize(new TableReference("non_existing_keyspace", "non_existing_table"))).isEqualTo(0);
        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(0);
    }

    @Test
    public void testTableStatesUnableToConnectToJmx() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long tableDataSize = 1000;

        doThrow(IOException.class).when(myJmxProxyFactory).connect();

        mockTable(tableReference, tableDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(0);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(0);
    }

    @Test
    public void testTableStatesUpdateUnableToConnectToJmx() throws IOException
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long expectedDataSize = 1000;
        long expectedTableDataSize = 1000;
        long notExpectedDataSize = 1500;

        mockTable(tableReference, expectedTableDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(expectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(expectedTableDataSize);

        doThrow(IOException.class).when(myJmxProxyFactory).connect();

        mockTable(tableReference, notExpectedDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(expectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(expectedTableDataSize);
    }

    @Test
    public void testTableStatesForUnexpectedTable()
    {
        TableReference unexpectedTableReference = new TableReference("keyspace", "nonexistingtable");

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize(unexpectedTableReference)).isEqualTo(0);
    }

    @Test
    public void testTableStatesForSingleTable()
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long expectedDataSize = 1000;
        long expectedTableDataSize = 1000;

        mockTable(tableReference, expectedTableDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(expectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(expectedTableDataSize);
    }

    @Test
    public void testTableStatesUpdatesForSingleTable()
    {
        TableReference tableReference = new TableReference("keyspace", "table");
        long expectedDataSize = 1000;
        long expectedTableDataSize = 1000;

        long newExpectedDataSize = 1500;
        long newExpectedTableDataSize = 1500;

        mockTable(tableReference, expectedTableDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(expectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(expectedTableDataSize);

        // Update table state

        mockTable(tableReference, newExpectedTableDataSize);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(newExpectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference)).isEqualTo(newExpectedTableDataSize);
    }

    @Test
    public void testTableStatesForMultipleTables()
    {
        TableReference tableReference1 = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");
        long expectedDataSize = 1500;
        long expectedTableDataSize1 = 1000;
        long expectedTableDataSize2 = 500;

        mockTable(tableReference1, expectedTableDataSize1);
        mockTable(tableReference2, expectedTableDataSize2);

        myTableStorageeStatesImpl.updateTableStates();

        assertThat(myTableStorageeStatesImpl.getDataSize()).isEqualTo(expectedDataSize);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference1)).isEqualTo(expectedTableDataSize1);
        assertThat(myTableStorageeStatesImpl.getDataSize(tableReference2)).isEqualTo(expectedTableDataSize2);
    }

    private void mockTable(TableReference tableReference, long dataSize)
    {
        myReplicatedTables.add(tableReference);
        doReturn(dataSize).when(myJmxProxy).liveDiskSpaceUsed(eq(tableReference));
    }
}
