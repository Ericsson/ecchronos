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
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.karaf.shell.support.CommandException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.TestRepairStatusCommand.createTableRef;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairTableStatusCommand
{
    private static final VnodeRepairState state1 = new VnodeRepairState(new LongTokenRange(5, 6), mockHosts("host1", "host2"), toMillis("2019-12-24T14:57:00Z"));
    private static final VnodeRepairState state2 = new VnodeRepairState(new LongTokenRange(1, 2), mockHosts("host1", "host3"), toMillis("2019-11-12T00:26:59Z"));
    private static final VnodeRepairState state3 = new VnodeRepairState(new LongTokenRange(3, 4), mockHosts("host2", "host3"), toMillis("1970-01-01T00:00:00Z"));

    private static final VnodeRepairStates states = createRepairStates(state1, state2, state3);

    @BeforeClass
    public static void setup()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Test
    public void testThatGetVnodeRepairStatesFindsMatch() throws CommandException
    {
        // Given
        RepairTableStatusCommand command = mockCommand("KS1.TBL1");
        // When
        VnodeRepairStates vnodeRepairStates = command.getVnodeRepairStates();
        // Then
        assertThat(vnodeRepairStates).isSameAs(states);
    }

    @Test
    public void testThatGetVnodeRepairStatesThrowsWhenTableDoesNotExist()
    {
        // Given
        RepairTableStatusCommand command = mockCommand("NonExisting.Table");
        // When looking for a non existing table - Then an exception should be thrown
        assertThatExceptionOfType(CommandException.class)
                .isThrownBy(command::getVnodeRepairStates)
                .withMessage("Table reference 'NonExisting.Table' was not found. Format must be <keyspace>.<table>");
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(String sortBy, boolean reverse, List<VnodeRepairState> expected)
    {
        // Given
        RepairTableStatusCommand command = mockCommand(sortBy, reverse, 0);
        List<VnodeRepairState> jobs = asList(state1, state2, state3);
        // When
        Comparator<VnodeRepairState> comparator = command.getRepairStateComparator();
        jobs.sort(comparator);
        // Then
        assertThat(jobs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {"RANGE", false, asList(state2, state3, state1)},
                {"RANGE", true, asList(state1, state3, state2)},
                {"REPAIRED_AT", false, asList(state3, state2, state1)},
                {"REPAIRED_AT", true, asList(state1, state2, state3)},
                {"DEFAULT", false, asList(state2, state3, state1)},
        };
    }

    @Test
    public void testTableStatusSortedByRange()
    {
        // Given
        RepairTableStatusCommand command = mockCommand("RANGE", false, 4);
        Comparator<VnodeRepairState> comparator = command.getRepairStateComparator();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printTableRanges(out, states, comparator);
        // Then
        String expected =
                "Range │ Last repaired at    │ Replicas\n" +
                "──────┼─────────────────────┼───────────────\n" +
                "(1,2] │ 2019-11-12 00:26:59 │ [host1, host3]\n" +
                "(3,4] │ 1970-01-01 00:00:00 │ [host2, host3]\n" +
                "(5,6] │ 2019-12-24 14:57:00 │ [host1, host2]\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    @Test
    public void testTableStatusSortedRepairedAtReversedAndLimited()
    {
        // Given
        RepairTableStatusCommand command = mockCommand("REPAIRED_AT", true, 2);
        Comparator<VnodeRepairState> comparator = command.getRepairStateComparator();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        command.printTableRanges(out, states, comparator);
        // Then
        String expected =
                "Range │ Last repaired at    │ Replicas\n" +
                "──────┼─────────────────────┼───────────────\n" +
                "(5,6] │ 2019-12-24 14:57:00 │ [host1, host2]\n" +
                "(1,2] │ 2019-11-12 00:26:59 │ [host1, host3]\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    private static ImmutableSet<Host> mockHosts(String... hosts)
    {
        ImmutableSet.Builder<Host> builder = ImmutableSet.builder();
        Stream.of(hosts)
                .map(TestRepairTableStatusCommand::mockHost)
                .forEach(builder::add);
        return builder.build();
    }

    private static Host mockHost(String hostName)
    {
        Host host = mock(Host.class);
        when(host.toString()).thenReturn(hostName);
        return host;
    }

    private static VnodeRepairStates createRepairStates(VnodeRepairState... states)
    {
        return VnodeRepairStates.newBuilder(asList(states)).build();
    }

    private static RepairJobView mockRepairJob(VnodeRepairStates vnodeRepairStates)
    {
        TableReference tableReference = createTableRef("ks1.tbl1");
        RepairStateSnapshot state = mock(RepairStateSnapshot.class);
        when(state.getVnodeRepairStates()).thenReturn(vnodeRepairStates);
        return new RepairJobView(tableReference, null, state);
    }

    private RepairTableStatusCommand mockCommand(String tableRef)
    {
        return new RepairTableStatusCommand(mockScheduler(), tableRef, null, false, 0);
    }

    private RepairTableStatusCommand mockCommand(String sortBy, boolean reverse, int limit)
    {
        return new RepairTableStatusCommand(mockScheduler(), "Table", sortBy, reverse, limit);
    }

    private RepairScheduler mockScheduler()
    {
        RepairJobView repairJobView = mockRepairJob(states);
        RepairScheduler schedulerMock = mock(RepairScheduler.class);
        when(schedulerMock.getCurrentRepairJobs()).thenReturn(asList(repairJobView));
        return schedulerMock;
    }

    private static long toMillis(String date)
    {
        return Instant.parse(date).toEpochMilli();
    }
}
