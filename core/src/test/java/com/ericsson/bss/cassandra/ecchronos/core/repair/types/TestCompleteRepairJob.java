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
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob.Status;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCompleteRepairJob
{
    @Test
    public void testFullyRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Collections.singletonList(vnodeRepairState));

        CompleteRepairJob completeRepairJob = new CompleteRepairJob(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairedRatio).isEqualTo(1.0d);
        assertThat(completeRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(completeRepairJob.status).isEqualTo(Status.COMPLETED);
        assertThat(completeRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState);
    }

    @Test
    public void testNotRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(9);

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Collections.singletonList(vnodeRepairState));

        CompleteRepairJob completeRepairJob = new CompleteRepairJob(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairedRatio).isEqualTo(0.0d);
        assertThat(completeRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(completeRepairJob.status).isEqualTo(Status.WARNING);
        assertThat(completeRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState);
    }

    @Test
    public void testHalfRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(11);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, ImmutableSet.of(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, ImmutableSet.of(), lastRepairedAtSecond);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Arrays.asList(vnodeRepairState, vnodeRepairState2));

        CompleteRepairJob completeRepairJob = new CompleteRepairJob(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairedRatio).isEqualTo(0.5d);
        assertThat(completeRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(completeRepairJob.status).isEqualTo(Status.ERROR);
        assertThat(completeRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState, vnodeRepairState2);
    }

    @Test
    public void testInQueueStatus()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(5);
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(6);
        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval);

        CompleteRepairJob completeRepairJob = new CompleteRepairJob(repairJobView);

        assertThat(completeRepairJob.lastRepairedAtInMs).isEqualTo(lastRepairedAt);
        assertThat(completeRepairJob.status).isEqualTo(Status.IN_QUEUE);
        assertThat(completeRepairJob.nextRepairInMs).isEqualTo(lastRepairedAt + repairInterval);
    }

    private void assertVnodes(CompleteRepairJob completeRepairJob, long repairedAfter, VnodeRepairState... vnodeRepairStates)
    {
        assertThat(completeRepairJob.virtualNodeStates).hasSize(vnodeRepairStates.length);

        for (int i = 0; i < vnodeRepairStates.length; i++)
        {
            VirtualNodeState vnodeState = VirtualNodeState.convert(vnodeRepairStates[i], repairedAfter);

            assertThat(completeRepairJob.virtualNodeStates.get(i)).isEqualToComparingFieldByField(vnodeState);
        }
    }
}
