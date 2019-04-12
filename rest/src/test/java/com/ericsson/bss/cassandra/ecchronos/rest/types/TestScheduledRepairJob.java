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
package com.ericsson.bss.cassandra.ecchronos.rest.types;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.rest.TestUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestScheduledRepairJob
{
    @Test
    public void testFullyRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis();
        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval);

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(1.0d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
    }

    @Test
    public void testNotRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);
        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval);

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
    }

    @Test
    public void testHalfRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Sets.newHashSet(vnodeRepairState, vnodeRepairState2));

        ScheduledRepairJob scheduledRepairJob = new ScheduledRepairJob(repairJobView);

        assertThat(scheduledRepairJob.keyspace).isEqualTo("ks");
        assertThat(scheduledRepairJob.table).isEqualTo("tb");
        assertThat(scheduledRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(scheduledRepairJob.repairedRatio).isEqualTo(0.5d);
        assertThat(scheduledRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
    }
}
