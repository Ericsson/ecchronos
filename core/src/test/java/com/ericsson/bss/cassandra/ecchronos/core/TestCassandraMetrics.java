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

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestCassandraMetrics
{
    @Mock
    private JmxProxyFactory myJmxProxyFactoryMock;
    @Mock
    private JmxProxy myJmxProxyMock;

    private CassandraMetrics myCassandraMetrics;

    @Before
    public void setup() throws Exception
    {
        doReturn(myJmxProxyMock).when(myJmxProxyFactoryMock).connect();
        myCassandraMetrics = new CassandraMetrics(myJmxProxyFactoryMock);
    }

    @After
    public void teardown()
    {
        myCassandraMetrics.close();
    }

    @Test
    public void testLoadMetricsFails() throws IOException
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long maxRepairedAt = 1234L;
        double percentRepaired = 0.5d;

        doThrow(IOException.class).when(myJmxProxyFactoryMock).connect();

        mockTable(tableReference, maxRepairedAt, percentRepaired);

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(0L);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(0.0d);
        verify(myJmxProxyFactoryMock, times(2)).connect();
    }

    @Test
    public void testUpdateMetricsTwiceUnableToConnectToJmxSecondTime()
            throws IOException
    {
        TableReference tableReference = tableReference("keyspace", "table");
        long firstMaxRepairedAt = 1234L;
        double firstPercentRepaired = 0.5d;

        long secondMaxRepairedAt = 2345L;
        double secondPercentRepaired = 1.0d;

        mockTable(tableReference, firstMaxRepairedAt, firstPercentRepaired);

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(firstMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(firstPercentRepaired);

        doThrow(IOException.class).when(myJmxProxyFactoryMock).connect();
        mockTable(tableReference, secondMaxRepairedAt, secondPercentRepaired);
        myCassandraMetrics.refreshCache(tableReference);

        assertThat(myCassandraMetrics.getMaxRepairedAt(tableReference)).isEqualTo(firstMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(tableReference)).isEqualTo(firstPercentRepaired);
        verify(myJmxProxyFactoryMock, times(2)).connect();
    }

    @Test
    public void testUpdateMetricsForMultipleTables() throws IOException
    {
        TableReference firstTable = tableReference("keyspace", "table");
        long firstTableMaxRepairedAt = 1234L;
        double firstTablePercentRepaired = 0.5d;
        mockTable(firstTable, firstTableMaxRepairedAt, firstTablePercentRepaired);

        assertThat(myCassandraMetrics.getMaxRepairedAt(firstTable)).isEqualTo(firstTableMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(firstTable)).isEqualTo(firstTablePercentRepaired);

        TableReference secondTable = tableReference("keyspace", "table2");
        long secondTableMaxRepairedAt = 2345L;
        double secondTablePercentRepaired = 1.0d;
        mockTable(secondTable, secondTableMaxRepairedAt, secondTablePercentRepaired);

        assertThat(myCassandraMetrics.getMaxRepairedAt(secondTable)).isEqualTo(secondTableMaxRepairedAt);
        assertThat(myCassandraMetrics.getPercentRepaired(secondTable)).isEqualTo(secondTablePercentRepaired);
        verify(myJmxProxyFactoryMock, times(2)).connect();
    }

    private void mockTable(TableReference tableReference, long maxRepairedAt, double percentRepaired)
    {
        doReturn(maxRepairedAt).when(myJmxProxyMock).getMaxRepairedAt(eq(tableReference));
        doReturn(percentRepaired).when(myJmxProxyMock).getPercentRepaired(eq(tableReference));
    }
}
