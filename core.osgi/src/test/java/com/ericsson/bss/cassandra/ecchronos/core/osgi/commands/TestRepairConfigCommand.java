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
package com.ericsson.bss.cassandra.ecchronos.core.osgi.commands;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.UUID;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.ScheduledRepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.TestRepairStatusCommand.createTableRef;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith (MockitoJUnitRunner.class)
public class TestRepairConfigCommand
{
    private static final long time1 = Duration.ofHours(6).toMillis();
    private static final long time2 = Duration.ofDays(3).plusHours(2).plusMinutes(33).plusSeconds(44).toMillis();
    private static final long time3 = Duration.ofDays(7).toMillis();

    @Mock
    RepairScheduler schedulerMock;

    @Mock
    private static RepairStateSnapshot repairStateSnapshotMock;

    @Test
    public void testRepairConfigSortedByTableName()
    {
        // Given
        ScheduledRepairJobView job1 = mockRepairJob("ks1.tbl1", time1, 0.1, time3, time2);
        ScheduledRepairJobView job2 = mockRepairJob("ks2.tbl1", time2, 0.2, time1, time3);
        ScheduledRepairJobView job3 = mockRepairJob("ks1.tbl2", time3, 0.3, time2, time1);

        when(schedulerMock.getCurrentRepairJobs()).thenReturn(asList(job1, job2, job3));

        RepairConfigCommand command = new RepairConfigCommand(schedulerMock);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printConfig(out);
        // Then
        String expected =
                "Table name │ Interval      │ Parallelism │ Unwind ratio │ Warning time  │ Error time\n" +
                "───────────┼───────────────┼─────────────┼──────────────┼───────────────┼──────────────\n" +
                "ks1.tbl1   │ 6h            │ PARALLEL    │ 10%          │ 7d            │ 3d 2h 33m 44s\n" +
                "ks1.tbl2   │ 7d            │ PARALLEL    │ 30%          │ 3d 2h 33m 44s │ 6h\n" +
                "ks2.tbl1   │ 3d 2h 33m 44s │ PARALLEL    │ 20%          │ 6h            │ 7d\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    private static ScheduledRepairJobView mockRepairJob(String table, long repairInterval, double unwindRatio, long warningTime, long errorTime)
    {
        TableReference tableReference = createTableRef(table);

        RepairConfiguration repairConfiguration = mock(RepairConfiguration.class);
        when(repairConfiguration.getRepairIntervalInMs()).thenReturn(repairInterval);
        when(repairConfiguration.getRepairParallelism()).thenReturn(RepairOptions.RepairParallelism.PARALLEL);
        when(repairConfiguration.getRepairUnwindRatio()).thenReturn(unwindRatio);
        when(repairConfiguration.getRepairWarningTimeInMs()).thenReturn(warningTime);
        when(repairConfiguration.getRepairErrorTimeInMs()).thenReturn(errorTime);

        return new ScheduledRepairJobView(UUID.randomUUID(), tableReference, repairConfiguration, repairStateSnapshotMock, ScheduledRepairJobView.Status.ON_TIME, 0, 0, RepairOptions.RepairType.VNODE);
    }
}
