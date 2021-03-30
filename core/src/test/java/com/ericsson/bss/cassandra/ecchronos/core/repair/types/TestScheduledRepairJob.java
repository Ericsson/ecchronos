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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TestUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView.Status;
import static org.assertj.core.api.Assertions.assertThat;

public class TestScheduledRepairJob
{
    @Test
    public void testFullyRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();
        UUID id = UUID.randomUUID();
        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withStatus(Status.COMPLETED)
                .withProgress(1.0d)
                .build();

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.id).isEqualTo(id);
        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(1.0d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.COMPLETED);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testNotRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(5);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        UUID id = UUID.randomUUID();
        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .build();

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.id).isEqualTo(id);
        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.IN_QUEUE);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testHalfRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(9);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);

        UUID id = UUID.randomUUID();

        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withVnodeRepairStateSet(Arrays.asList(vnodeRepairState, vnodeRepairState2))
                .withStatus(Status.WARNING)
                .withProgress(0.5d)
                .build();

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.id).isEqualTo(id);
        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.5d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.WARNING);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testErrorStatus()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(11);
        UUID id = UUID.randomUUID();
        RepairJobView repairJobView = new TestUtils.ScheduledRepairJobBuilder()
                .withId(id)
                .withKeyspace("ks")
                .withTable("tb")
                .withLastRepairedAt(lastRepairedAt)
                .withRepairInterval(repairInterval)
                .withStatus(Status.ERROR)
                .build();
        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(scheduledRepairJob.status).isEqualTo(Status.ERROR);
        assertThat(scheduledRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(ScheduledRepairJob.class).usingGetClass()
                .withNonnullFields("id", "keyspace", "table")
                .verify();
    }
}
