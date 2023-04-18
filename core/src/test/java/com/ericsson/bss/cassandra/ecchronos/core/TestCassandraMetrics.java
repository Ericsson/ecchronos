/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@RunWith(MockitoJUnitRunner.class)
public class TestCassandraMetrics
{
    @Mock
    private JmxProxyFactory myJmxProxyFactoryMock;
    @Mock
    private JmxProxy myJmxProxyMock;
    @Mock
    private ReplicatedTableProvider myReplicatedTableProviderMock;

    private Set<TableReference> myReplicatedTables = new HashSet<>();
    private CassandraMetrics myCassandraMetrics;

    @Before
    public void setup() throws Exception
    {
        doReturn(myJmxProxyMock).when(myJmxProxyFactoryMock).connect();
        doReturn(myReplicatedTables).when(myReplicatedTableProviderMock).getAll();
        myCassandraMetrics = CassandraMetrics.builder()
                .withJmxProxyFactory(myJmxProxyFactoryMock)
                .withReplicatedTableProvider(myReplicatedTableProviderMock)
                .withInitialDelay(60, TimeUnit.SECONDS)
                .build();
    }

    @After
    public void teardown()
    {
        myCassandraMetrics.close();
    }

    @Test
    public void testGetMetricsForNonExistingTable()
    {
        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference("non_existing_keyspace", "non_existing_table")))
                .isEqualTo(0L);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference("non_existing_keyspace", "non_existing_table"))).isEqualTo(0.0d);
    }

    @Test
    public void testUpdateMetricsUnableToConnectToJmx() throws IOException
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long maxRepairedAt = 1234L;
        double percentRepaired = 0.5d;

        doThrow(IOException.class).when(myJmxProxyFactoryMock).connect();

        mockTable(tableReference, maxRepairedAt, percentRepaired);

        myCassandraMetrics.updateMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(0L);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(0.0d);
    }

    @Test
    public void testUpdateMetricsTwiceUnableToConnectToJmxSecondTime() throws IOException
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long firstMaxRepairedAt = 1234L;
        double firstPercentRepaired = 0.5d;

        long secondMaxRepairedAt = 2345L;
        double secondPercentRepaired = 1.0d;

        mockTable(tableReference, firstMaxRepairedAt, firstPercentRepaired);

        myCassandraMetrics.updateMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(firstMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(firstPercentRepaired);

        doThrow(IOException.class).when(myJmxProxyFactoryMock).connect();

        mockTable(tableReference, secondMaxRepairedAt, secondPercentRepaired);

        myCassandraMetrics.updateMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(firstMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(firstPercentRepaired);
    }

    @Test
    public void testUpdateMetricsForMultipleTables()
    {
        TableReference firstTable = tableReference("keyspace", "table");
        long firstTableMaxRepairedAt = 1234L;
        double firstTablePercentRepaired = 0.5d;
        mockTable(firstTable, firstTableMaxRepairedAt, firstTablePercentRepaired);

        TableReference secondTable = tableReference("keyspace", "table2");
        long secondTableMaxRepairedAt = 2345L;
        double secondTablePercentRepaired = 1.0d;
        mockTable(secondTable, secondTableMaxRepairedAt, secondTablePercentRepaired);

        myCassandraMetrics.updateMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(firstTable)).isEqualTo(firstTableMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(firstTable)).isEqualTo(firstTablePercentRepaired);

        assertThat(myCassandraMetrics.getMaxRepairedAt(secondTable)).isEqualTo(secondTableMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(secondTable)).isEqualTo(secondTablePercentRepaired);
    }

    private void mockTable(TableReference tableReference, long maxRepairedAt, double percentRepaired)
    {
        myReplicatedTables.add(tableReference);
        doReturn(maxRepairedAt).when(myJmxProxyMock).getMaxRepairedAt(eq(tableReference));
        doReturn(percentRepaired).when(myJmxProxyMock).getPercentRepaired(eq(tableReference));
    }
}
