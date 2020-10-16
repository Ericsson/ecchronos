/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairHistory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import org.assertj.core.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith(MockitoJUnitRunner.class)
public class TestOnDemandRepairSchedulerImpl
{
    private static final TableReference TABLE_REFERENCE = tableReference("keyspace", "table");

    @Mock
    private JmxProxyFactory jmxProxyFactory;

    @Mock
    private ScheduleManager scheduleManager;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    @Mock
    private ReplicationState replicationState;

    @Mock
    private RepairHistory repairHistory;

    @Mock
    private Metadata metadata;

    @Mock
    private NativeConnectionProvider myNativeConnectionProvider;

    @Mock
    private Host myHost;

    @Mock
    private Session mySession;

    @Mock
    private PreparedStatement myPreparedStatement;

    @Mock
    private Cluster myCluster;

    @Mock
    private Metadata myMetadata;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private TableMetadata myTableMetadata;

    @Mock
    private UserType myUserType;

    @Mock
    private UDTValue myUDTValue;

    @Mock
    private ResultSet myReultSet;

    @Mock
    private Row myRow;

    @Before
    public void setup()
    {
        when(myNativeConnectionProvider.getLocalHost()).thenReturn(myHost);
        when(myNativeConnectionProvider.getSession()).thenReturn(mySession);
        when(mySession.prepare((RegularStatement)any())).thenReturn(myPreparedStatement);
        when(mySession.getCluster()).thenReturn(myCluster);
        when(mySession.execute((Statement)any())).thenReturn(myReultSet);
        when(myCluster.getMetadata()).thenReturn(myMetadata);
        when(myMetadata.getKeyspace(any())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getUserType(any())).thenReturn(myUserType);
        when(myUserType.newValue()).thenReturn(myUDTValue);
        when(myUDTValue.setUUID(anyString(), any())).thenReturn(myUDTValue);
        when(myUDTValue.setString(anyString(), anyString())).thenReturn(myUDTValue);
        when(myPreparedStatement.setConsistencyLevel(any())).thenReturn(myPreparedStatement);
    }

    @Test
    public void testScheduleRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(myTableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView);

        repairScheduler.close();
        verify(scheduleManager).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testScheduleTwoRepairOnTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(myTableMetadata);

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        RepairJobView repairJobView = repairScheduler.scheduleJob(TABLE_REFERENCE);
        RepairJobView repairJobView2 = repairScheduler.scheduleJob(TABLE_REFERENCE);
        verify(scheduleManager, times(2)).schedule(any(ScheduledJob.class));

        assertTableViewExist(repairScheduler, repairJobView, repairJobView2);

        repairScheduler.close();
        verify(scheduleManager, times(2)).deschedule(any(ScheduledJob.class));

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        verifyNoMoreInteractions(scheduleManager);
    }

    @Test
    public void testRestartRepairOnTable() throws EcChronosException
    {
        List<Row> rowList = new ArrayList<>();
        rowList.add(myRow);
        when(myReultSet.all()).thenReturn(rowList);
        when(myRow.getString(eq("status"))).thenReturn("started");
        when(myRow.getUUID(eq("job_id"))).thenReturn(TABLE_REFERENCE.getId());
        Set<UDTValue> repairedSet = new HashSet<>();
        when(myRow.getSet("repaired_tokens", UDTValue.class)).thenReturn(repairedSet);
        when(myRow.getUDTValue(eq("table_reference"))).thenReturn(myUDTValue);
        when(myUDTValue.getString(eq("keyspace_name"))).thenReturn(TABLE_REFERENCE.getKeyspace());
        when(myUDTValue.getString(eq("table_name"))).thenReturn(TABLE_REFERENCE.getTable());
        when(myUDTValue.getUUID(eq("id"))).thenReturn(TABLE_REFERENCE.getId());
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        when(myKeyspaceMetadata.getTable(TABLE_REFERENCE.getTable())).thenReturn(myTableMetadata);
        when(myKeyspaceMetadata.getName()).thenReturn(TABLE_REFERENCE.getKeyspace());
        when(myTableMetadata.getId()).thenReturn(TABLE_REFERENCE.getId());
        when(myTableMetadata.getKeyspace()).thenReturn(myKeyspaceMetadata);
        when(myTableMetadata.getName()).thenReturn(TABLE_REFERENCE.getTable());
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager).schedule(any(ScheduledJob.class));
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentKeyspaceTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();

        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    @Test (expected = EcChronosException.class)
    public void testScheduleRepairOnNonExistentTable() throws EcChronosException
    {
        OnDemandRepairSchedulerImpl repairScheduler = defaultOnDemandRepairSchedulerImplBuilder().build();
        when(metadata.getKeyspace(TABLE_REFERENCE.getKeyspace())).thenReturn(myKeyspaceMetadata);
        verify(scheduleManager, never()).schedule(any(ScheduledJob.class));
        repairScheduler.scheduleJob(TABLE_REFERENCE);
    }

    private void assertTableViewExist(OnDemandRepairScheduler repairScheduler, RepairJobView... expectedViews)
    {
        List<RepairJobView> repairJobViews = repairScheduler.getCurrentRepairJobs();
        assertThat(repairJobViews).containsExactlyInAnyOrder(expectedViews);
    }

    private OnDemandRepairSchedulerImpl.Builder defaultOnDemandRepairSchedulerImplBuilder()
    {
        return OnDemandRepairSchedulerImpl.builder()
                .withJmxProxyFactory(jmxProxyFactory)
                .withTableRepairMetrics(myTableRepairMetrics)
                .withScheduleManager(scheduleManager)
                .withReplicationState(replicationState)
                .withMetadata(metadata)
                .withRepairLockType(RepairLockType.VNODE)
                .withRepairConfiguration(RepairConfiguration.DEFAULT)
                .withRepairHistory(repairHistory)
                .withNativeConnectionProvider(myNativeConnectionProvider);
    }
}
