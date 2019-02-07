/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.rest.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.rest.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.rest.types.VnodeState;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.assertj.core.util.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairSchedulerRESTImpl
{
    @Mock
    private RepairScheduler myRepairScheduler;

    @Test
    public void testListEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = new Gson().fromJson(repairSchedulerService.list(), new TypeToken<List<ScheduledRepairJob>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testListEntry()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = new Gson().fromJson(repairSchedulerService.list(), new TypeToken<List<ScheduledRepairJob>>(){}.getType());

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAt).isEqualTo(expectedLastRepairedAt);
        assertThat(scheduledRepairJob.repaired).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.repairIntervalInMs).isEqualTo(expectedRepairInterval);
    }

    @Test
    public void testListKeyspaceEmpty()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = new Gson().fromJson(repairSchedulerService.listKeyspace(""), new TypeToken<List<ScheduledRepairJob>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceNonExisting()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = new Gson().fromJson(repairSchedulerService.listKeyspace("nonexistingkeyspace"), new TypeToken<List<ScheduledRepairJob>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testListKeyspaceEntry()
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval);

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        List<ScheduledRepairJob> response = new Gson().fromJson(repairSchedulerService.listKeyspace("ks"), new TypeToken<List<ScheduledRepairJob>>(){}.getType());

        assertThat(response).hasSize(1);

        ScheduledRepairJob scheduledRepairJob = response.get(0);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.lastRepairedAt).isEqualTo(expectedLastRepairedAt);
        assertThat(scheduledRepairJob.repaired).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.repairIntervalInMs).isEqualTo(expectedRepairInterval);
    }

    @Test
    public void testGetTableNonExisting()
    {
        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(new ArrayList<>());

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        Map<Object, Object> response = new Gson().fromJson(repairSchedulerService.get("ks", "tb"), new TypeToken<Map<Object, Object>>(){}.getType());

        assertThat(response).isEmpty();
    }

    @Test
    public void testGetTableEntry() throws UnknownHostException
    {
        long expectedLastRepairedAt = 234;
        long expectedRepairInterval = 123;
        LongTokenRange longTokenRange = new LongTokenRange(2, 3);
        Host host = mock(Host.class);
        when(host.getBroadcastAddress()).thenReturn(InetAddress.getLocalHost());

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", expectedLastRepairedAt, expectedRepairInterval,
                longTokenRange, Sets.newLinkedHashSet(host));

        when(myRepairScheduler.getCurrentRepairJobs()).thenReturn(Collections.singletonList(repairJobView));

        RepairSchedulerREST repairSchedulerService = new RepairSchedulerRESTImpl(myRepairScheduler);

        CompleteRepairJob response = new Gson().fromJson(repairSchedulerService.get("ks", "tb"), CompleteRepairJob.class);

        assertThat(response.keyspace).isEqualTo("ks");
        assertThat(response.table).isEqualTo("tb");
        assertThat(response.lastRepairedAt).isEqualTo(expectedLastRepairedAt);
        assertThat(response.repaired).isEqualTo(0.0d);
        assertThat(response.repairIntervalInMs).isEqualTo(expectedRepairInterval);
        assertThat(response.vnodeStates).hasSize(1);

        VnodeState vnodeState = response.vnodeStates.get(0);
        assertThat(vnodeState.startToken).isEqualTo(longTokenRange.start.getValue());
        assertThat(vnodeState.endToken).isEqualTo(longTokenRange.end.getValue());
        assertThat(vnodeState.lastRepairedAt).isEqualTo(expectedLastRepairedAt);
        assertThat(vnodeState.replicas).isEqualTo(Sets.newLinkedHashSet(InetAddress.getLocalHost()));
    }
}
