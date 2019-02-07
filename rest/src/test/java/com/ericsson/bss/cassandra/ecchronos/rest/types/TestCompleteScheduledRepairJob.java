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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCompleteScheduledRepairJob
{
    @Test
    public void testFullyRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, Collections.emptySet(), lastRepairedAt);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Collections.singletonList(vnodeRepairState));

        CompleteRepairJob completeRepairJob = CompleteRepairJob.convert(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(completeRepairJob.repaired).isEqualTo(1.0d);
        assertThat(completeRepairJob.lastRepairedAt).isEqualTo(lastRepairedAt);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState);
    }

    @Test
    public void testNotRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, Collections.emptySet(), lastRepairedAt);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Collections.singletonList(vnodeRepairState));

        CompleteRepairJob completeRepairJob = CompleteRepairJob.convert(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(completeRepairJob.repaired).isEqualTo(0.0d);
        assertThat(completeRepairJob.lastRepairedAt).isEqualTo(lastRepairedAt);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState);
    }

    @Test
    public void testHalfRepairedJob()
    {
        long repairInterval = TimeUnit.DAYS.toMillis(7);
        long repairedAfter = System.currentTimeMillis() - repairInterval;
        long lastRepairedAt = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);
        long lastRepairedAtSecond = System.currentTimeMillis();

        VnodeRepairState vnodeRepairState = TestUtils.createVnodeRepairState(1, 2, Collections.emptySet(), lastRepairedAt);
        VnodeRepairState vnodeRepairState2 = TestUtils.createVnodeRepairState(3, 4, Collections.emptySet(), lastRepairedAtSecond);

        RepairJobView repairJobView = TestUtils.createRepairJob("ks", "tb", lastRepairedAt, repairInterval, Arrays.asList(vnodeRepairState, vnodeRepairState2));

        CompleteRepairJob completeRepairJob = CompleteRepairJob.convert(repairJobView);

        assertThat(completeRepairJob.keyspace).isEqualTo("ks");
        assertThat(completeRepairJob.table).isEqualTo("tb");
        assertThat(completeRepairJob.repairIntervalInMs).isEqualTo(repairInterval);
        assertThat(completeRepairJob.repaired).isEqualTo(0.5d);
        assertThat(completeRepairJob.lastRepairedAt).isEqualTo(lastRepairedAt);

        assertVnodes(completeRepairJob, repairedAfter, vnodeRepairState, vnodeRepairState2);
    }

    private void assertVnodes(CompleteRepairJob completeRepairJob, long repairedAfter, VnodeRepairState... vnodeRepairStates)
    {
        assertThat(completeRepairJob.vnodeStates).hasSize(vnodeRepairStates.length);

        for (int i = 0; i < vnodeRepairStates.length; i++)
        {
            VnodeState vnodeState = VnodeState.convert(vnodeRepairStates[i], repairedAfter);

            assertThat(completeRepairJob.vnodeStates.get(i)).isEqualToComparingFieldByField(vnodeState);
        }
    }
}
