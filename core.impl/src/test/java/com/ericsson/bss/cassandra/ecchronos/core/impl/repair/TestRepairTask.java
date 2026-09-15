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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import static com.ericsson.bss.cassandra.ecchronos.core.impl.table.MockTableReferenceFactory.tableReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairStatus;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ScheduledJobException;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.management.NotificationListener;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairTask
{
    private final TableReference myTableReference = tableReference("keyspace", "table");
    private final UUID myNodeId = UUID.randomUUID();

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private DistributedJmxProxy myJmxProxy;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private RepairConfiguration myRepairConfiguration;

    @Before
    public void setup() throws Exception
    {
        when(myJmxProxyFactory.getMaxWaitTimeInMinutes()).thenReturn(40);
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
        when(myJmxProxy.addStorageServiceListener(any(UUID.class), any(NotificationListener.class))).thenReturn(true);
        myRepairConfiguration = RepairConfiguration.DEFAULT;
    }

    @Test
    public void testNonPositiveCommandFailsTask() throws Exception
    {
        // Cassandra did not start a repair session (e.g. incremental prepare-phase abort).
        when(myJmxProxy.repairAsync(eq(myNodeId), anyString(), any())).thenReturn(0);

        TestableRepairTask task = new TestableRepairTask();

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(task::execute);

        assertThat(task.finishStatus).isEqualTo(RepairStatus.FAILED);
    }

    @Test
    public void testNegativeCommandFailsTask() throws Exception
    {
        when(myJmxProxy.repairAsync(eq(myNodeId), anyString(), any())).thenReturn(-1);

        TestableRepairTask task = new TestableRepairTask();

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(task::execute);

        assertThat(task.finishStatus).isEqualTo(RepairStatus.FAILED);
    }

    @Test
    public void testFailedRangesFailTaskWithoutClusterWideTerminate() throws Exception
    {
        TestableRepairTask task = new TestableRepairTask();
        task.markRangeFailed(com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange.of(1, 100));

        assertThatExceptionOfType(ScheduledJobException.class)
                .isThrownBy(() -> task.invokeVerifyRepair(myJmxProxy))
                .withMessageContaining("failed ranges");

        // The failed-range path must not abort repairs cluster-wide (issue #1815).
        org.mockito.Mockito.verify(myJmxProxy, org.mockito.Mockito.never()).forceTerminateAllRepairSessions();
        org.mockito.Mockito.verify(myJmxProxy, org.mockito.Mockito.never())
                .forceTerminateAllRepairSessionsInSpecificNode(any(UUID.class));
    }

    private final class TestableRepairTask extends RepairTask
    {
        private volatile RepairStatus finishStatus;

        private TestableRepairTask()
        {
            super(myNodeId, myJmxProxyFactory, myTableReference, myRepairConfiguration, myTableRepairMetrics, 40);
        }

        void markRangeFailed(final com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange range)
        {
            onRangeFinished(range, RepairStatus.FAILED);
        }

        void invokeVerifyRepair(final DistributedJmxProxy proxy) throws ScheduledJobException
        {
            verifyRepair(proxy);
        }

        @Override
        protected Map<String, String> getOptions()
        {
            return Collections.emptyMap();
        }

        @Override
        protected void onFinish(final RepairStatus repairStatus)
        {
            finishStatus = repairStatus;
        }
    }
}
