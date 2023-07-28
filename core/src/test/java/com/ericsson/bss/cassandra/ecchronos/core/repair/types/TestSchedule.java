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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView.Status;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchedule
{
    @Test
    public void testFullyRepairedSchedule()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(11);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        UUID id = UUID.randomUUID();
        ScheduledRepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withStatus(Status.COMPLETED)
                .withProgress(1.0d)
                .withRepairConfiguration(repairConfig)
                .withVnodeRepairStateSet(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .withRepairType(RepairOptions.RepairType.VNODE)
                .build();

        Schedule schedule = new Schedule(repairJobView, true);

        assertThat(schedule.id).isEqualTo(id);
        assertThat(schedule.keyspace).isEqualTo("ks");
        assertThat(schedule.table).isEqualTo("tb");
        assertThat(schedule.repairedRatio).isEqualTo(1.0d);
        assertThat(schedule.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(schedule.status).isEqualTo(Status.COMPLETED);
        assertThat(schedule.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(schedule.config).isEqualTo(new ScheduleConfig(repairJobView));
        assertThat(schedule.repairType).isEqualTo(RepairOptions.RepairType.VNODE);

        assertVnodes(schedule, repairedAfter, vnodeRepairState, vnodeRepairState2);
    }

    @Test
    public void testNotRepairedSchedule()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(5);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);
        UUID id = UUID.randomUUID();
        ScheduledRepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withRepairConfiguration(repairConfig)
                .withRepairType(RepairOptions.RepairType.INCREMENTAL)
                .build();

        Schedule schedule = new Schedule(repairJobView);

        assertThat(schedule.id).isEqualTo(id);
        assertThat(schedule.keyspace).isEqualTo("ks");
        assertThat(schedule.table).isEqualTo("tb");
        assertThat(schedule.repairedRatio).isEqualTo(0.0d);
        assertThat(schedule.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(schedule.status).isEqualTo(Status.ON_TIME);
        assertThat(schedule.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(schedule.config).isEqualTo(new ScheduleConfig(repairJobView));
        assertThat(schedule.virtualNodeStates).isEmpty();
        assertThat(schedule.repairType).isEqualTo(RepairOptions.RepairType.INCREMENTAL);
    }

    @Test
    public void testHalfRepairedSchedule()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(9);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);
        RepairConfiguration repairConfig = TestUtils.generateRepairConfiguration(repairInterval);

        UUID id = UUID.randomUUID();

        ScheduledRepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withVnodeRepairStateSet(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .withStatus(Status.LATE)
                .withProgress(0.5d)
                .withRepairConfiguration(repairConfig)
                .withRepairType(RepairOptions.RepairType.VNODE)
                .build();

        Schedule schedule = new Schedule(repairJobView, true);

        assertThat(schedule.id).isEqualTo(id);
        assertThat(schedule.keyspace).isEqualTo("ks");
        assertThat(schedule.table).isEqualTo("tb");
        assertThat(schedule.repairedRatio).isEqualTo(0.5d);
        assertThat(schedule.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(schedule.status).isEqualTo(Status.LATE);
        assertThat(schedule.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(schedule.config).isEqualTo(new ScheduleConfig(repairJobView));
        assertThat(schedule.repairType).isEqualTo(RepairOptions.RepairType.VNODE);

        assertVnodes(schedule, repairedAfter, vnodeRepairState, vnodeRepairState2);
    }

    @Test
    public void testSchedule()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(9);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);
        RepairConfiguration repairConfig = TestUtils.createRepairConfiguration(11, 2.2, 33, 44);

        UUID id = UUID.randomUUID();

        ScheduledRepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withVnodeRepairStateSet(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .withStatus(Status.LATE)
                .withProgress(0.5d)
                .withRepairConfiguration(repairConfig)
                .withRepairType(RepairOptions.RepairType.VNODE)
                .build();

        Schedule schedule = new Schedule(repairJobView);

        assertThat(schedule.id).isEqualTo(id);
        assertThat(schedule.keyspace).isEqualTo("ks");
        assertThat(schedule.table).isEqualTo("tb");
        assertThat(schedule.repairedRatio).isEqualTo(0.5d);
        assertThat(schedule.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(schedule.status).isEqualTo(Status.LATE);
        assertThat(schedule.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
        assertThat(schedule.config).isEqualTo(new ScheduleConfig(repairJobView));
        assertThat(schedule.virtualNodeStates).isEmpty();
        assertThat(schedule.repairType).isEqualTo(RepairOptions.RepairType.VNODE);
    }


    @Test
    public void testErrorStatus()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(11);
        UUID id = UUID.randomUUID();
        ScheduledRepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withStatus(Status.OVERDUE)
                .withRepairType(RepairOptions.RepairType.VNODE)
                .build();
        Schedule schedule = new Schedule(repairJobView);

        assertThat(schedule.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(schedule.status).isEqualTo(Status.OVERDUE);
        assertThat(schedule.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.simple().forClass(Schedule.class).usingGetClass()
                .withNonnullFields("id", "keyspace", "table", "config", "virtualNodeStates", "repairType")
                .verify();
    }

    private void assertVnodes(Schedule schedule, long repairedAfter, VnodeRepairState... vnodeRepairStates)
    {
        assertThat(schedule.virtualNodeStates).hasSize(vnodeRepairStates.length);

        for (int i = 0; i < vnodeRepairStates.length; i++)
        {
            VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairStates[i], repairedAfter);
            assertThat(schedule.virtualNodeStates.get(i)).isEqualToComparingFieldByField(vnodeState);
        }
    }
}
