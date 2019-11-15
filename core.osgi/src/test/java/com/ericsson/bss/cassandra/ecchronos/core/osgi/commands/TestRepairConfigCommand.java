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
    @Mock
    RepairScheduler schedulerMock;

    @Test
    public void testRepairConfigSortedByTableName()
    {
        // Given
        RepairJobView job1 = mockRepairJob("ks1.tbl1", 11, 0.1, 111, 1111);
        RepairJobView job2 = mockRepairJob("ks2.tbl1", 22, 0.2, 222, 2222);
        RepairJobView job3 = mockRepairJob("ks1.tbl2", 33, 0.3, 333, 3333);

        when(schedulerMock.getCurrentRepairJobs()).thenReturn(asList(job1, job2, job3));

        RepairConfigCommand command = new RepairConfigCommand(schedulerMock);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printConfig(out);
        // Then
        String expected =
                "Table name │ Interval (ms) │ Parallelism │ Unwind ratio │ Error time (ms) │ Warning time (ms)\n" +
                "───────────┼───────────────┼─────────────┼──────────────┼─────────────────┼──────────────────\n" +
                "ks1.tbl1   │ 11            │ PARALLEL    │ 0.1          │ 111             │ 1111\n" +
                "ks1.tbl2   │ 33            │ PARALLEL    │ 0.3          │ 333             │ 3333\n" +
                "ks2.tbl1   │ 22            │ PARALLEL    │ 0.2          │ 222             │ 2222\n";
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
