/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class TestCassandraHealthIndicator
{
    @Mock
    private NativeConnectionProvider myNativeConnectionProvider;

    @Mock
    private JmxConnectionProvider myJmxConnectionProvider;

    @Mock
    private JMXConnector myJMXConnector;

    @Mock
    private CqlSession mySession;

    @Mock
    private ResultSet myResultSet;

    @Test
    public void testHealth() throws IOException
    {
        doReturn(myJMXConnector).when(myJmxConnectionProvider).getJmxConnector();
        doReturn(mock(MBeanServerConnection.class)).when(myJMXConnector).getMBeanServerConnection();
        doReturn(mySession).when(myNativeConnectionProvider).getSession();
        doReturn(myResultSet).when(mySession).execute(any(SimpleStatement.class));
        List<Row> rows = new ArrayList<>();
        rows.add(mock(Row.class));
        doReturn(rows).when(myResultSet).all();
        CassandraHealthIndicator cassandraHealthIndicator = new CassandraHealthIndicator(myNativeConnectionProvider,
                myJmxConnectionProvider);
        Health health = cassandraHealthIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    public void testHealthCQLDown() throws IOException
    {
        doReturn(myJMXConnector).when(myJmxConnectionProvider).getJmxConnector();
        doReturn(mock(MBeanServerConnection.class)).when(myJMXConnector).getMBeanServerConnection();
        doReturn(mySession).when(myNativeConnectionProvider).getSession();
        doThrow(AllNodesFailedException.fromErrors(new ArrayList<>())).when(mySession)
                .execute(any(SimpleStatement.class));
        CassandraHealthIndicator cassandraHealthIndicator = new CassandraHealthIndicator(myNativeConnectionProvider,
                myJmxConnectionProvider);
        Health health = cassandraHealthIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    public void testHealthCQLNoDataInSystemLocal() throws IOException
    {
        doReturn(myJMXConnector).when(myJmxConnectionProvider).getJmxConnector();
        doReturn(mock(MBeanServerConnection.class)).when(myJMXConnector).getMBeanServerConnection();
        doReturn(mySession).when(myNativeConnectionProvider).getSession();
        doReturn(myResultSet).when(mySession).execute(any(SimpleStatement.class));
        doReturn(Collections.emptyList()).when(myResultSet).all();
        CassandraHealthIndicator cassandraHealthIndicator = new CassandraHealthIndicator(myNativeConnectionProvider,
                myJmxConnectionProvider);
        Health health = cassandraHealthIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    public void testHealthJMXDown() throws IOException
    {
        doReturn(myJMXConnector).when(myJmxConnectionProvider).getJmxConnector();
        doThrow(new IOException("JMX is down")).when(myJMXConnector).getMBeanServerConnection();
        doReturn(mySession).when(myNativeConnectionProvider).getSession();
        doReturn(myResultSet).when(mySession).execute(any(SimpleStatement.class));
        List<Row> rows = new ArrayList<>();
        rows.add(mock(Row.class));
        doReturn(rows).when(myResultSet).all();
        CassandraHealthIndicator cassandraHealthIndicator = new CassandraHealthIndicator(myNativeConnectionProvider,
                myJmxConnectionProvider);
        Health health = cassandraHealthIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }
}
