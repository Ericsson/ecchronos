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
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
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

    @Test
    public void testRepairConfigSortedByTableName()
    {
        // Given
        RepairJobView job1 = mockRepairJob("ks1.tbl1", time1, 0.1, time2, time3);
        RepairJobView job2 = mockRepairJob("ks2.tbl1", time2, 0.2, time3, time1);
        RepairJobView job3 = mockRepairJob("ks1.tbl2", time3, 0.3, time1, time2);

        when(schedulerMock.getCurrentRepairJobs()).thenReturn(asList(job1, job2, job3));

        RepairConfigCommand command = new RepairConfigCommand(schedulerMock);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printConfig(out);
        // Then
        String expected =
                "Table name │ Interval      │ Parallelism │ Unwind ratio │ Error time    │ Warning time\n" +
                "───────────┼───────────────┼─────────────┼──────────────┼───────────────┼──────────────\n" +
                "ks1.tbl1   │ 6h            │ PARALLEL    │ 10%          │ 3d 2h 33m 44s │ 7d\n" +
                "ks1.tbl2   │ 7d            │ PARALLEL    │ 30%          │ 6h            │ 3d 2h 33m 44s\n" +
                "ks2.tbl1   │ 3d 2h 33m 44s │ PARALLEL    │ 20%          │ 7d            │ 6h\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    private static RepairJobView mockRepairJob(String table, long repairInterval, double unwindRatio, long errorTime, long warningTime)
    {
        TableReference tableReference = createTableRef(table);

        RepairConfiguration repairConfiguration = mock(RepairConfiguration.class);
        when(repairConfiguration.getRepairIntervalInMs()).thenReturn(repairInterval);
        when(repairConfiguration.getRepairParallelism()).thenReturn(RepairOptions.RepairParallelism.PARALLEL);
        when(repairConfiguration.getRepairUnwindRatio()).thenReturn(unwindRatio);
        when(repairConfiguration.getRepairErrorTimeInMs()).thenReturn(errorTime);
        when(repairConfiguration.getRepairWarningTimeInMs()).thenReturn(warningTime);

        return new RepairJobView(tableReference, repairConfiguration, null);
    }
}
