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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairHangMonitor
{
    private static final UUID NODE_ID = UUID.randomUUID();
    private static final int COMMAND = 42;
    private static final int MAX_WAIT_MINUTES = 10;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;

    @Mock
    private DistributedJmxProxy myProxy;

    @Mock
    private TableReference myTableReference;

    @Mock
    private RepairNotificationHandler myNotificationHandler;

    private RepairHangMonitor monitor;

    private ScheduledExecutorService testExecutor;

    @Before
    public void setup() throws IOException
    {
        when(myJmxProxyFactory.connect()).thenReturn(myProxy);
        when(myNotificationHandler.getCommand()).thenReturn(COMMAND);
        testExecutor = Executors.newSingleThreadScheduledExecutor();
        monitor = new RepairHangMonitor(myJmxProxyFactory, NODE_ID, myTableReference,
                MAX_WAIT_MINUTES, myNotificationHandler, 1, TimeUnit.SECONDS, testExecutor);
    }

    @After
    public void teardown()
    {
        monitor.cancel();
        testExecutor.shutdownNow();
    }

    @Test
    public void testNodeDownTerminatesRepair()
    {
        when(myProxy.getNodeStatus(NODE_ID)).thenReturn("DOWN");

        monitor.reschedule();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(myProxy, atLeastOnce()).forceTerminateAllRepairSessionsInSpecificNode(eq(NODE_ID));
            verify(myNotificationHandler, atLeastOnce()).countDown();
        });
    }

    @Test
    public void testRepairNoLongerActiveCountsDown()
    {
        when(myProxy.getNodeStatus(NODE_ID)).thenReturn("NORMAL");
        when(myProxy.isRepairActive(NODE_ID, COMMAND)).thenReturn(false);

        monitor.reschedule();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(myNotificationHandler, atLeastOnce()).countDown();
        });
    }

    @Test
    public void testNormalNodeWithActiveRepairReschedules()
    {
        when(myProxy.getNodeStatus(NODE_ID)).thenReturn("NORMAL");
        when(myProxy.isRepairActive(NODE_ID, COMMAND)).thenReturn(true);

        monitor.reschedule();

        // The monitor should check and reschedule - verify it at least checked
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(myProxy, atLeastOnce()).getNodeStatus(NODE_ID);
            verify(myProxy, atLeastOnce()).isRepairActive(NODE_ID, COMMAND);
        });
    }
}
