/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.MockedClock;
import com.ericsson.bss.cassandra.ecchronos.core.TokenUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@RunWith (MockitoJUnitRunner.class)
public class TestRepairStateImpl
{
    private static final long RUN_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(1);

    private static final long FORCE_REPAIR_TIME = TimeUnit.DAYS.toMillis(2);

    private static volatile InetAddress LOCAL_HOST;

    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private static final TableReference TABLE_REFERENCE = new TableReference(keyspaceName, tableName);

    @Mock
    private Metadata myMetadata;

    @Mock
    private KeyspaceMetadata myKeyspaceMetadata;

    @Mock
    private Host myHost;

    @Mock
    private Host mySecondHost;

    @Mock
    private Host myThirdHost;

    @Mock
    private HostStates myHostStates;

    @Mock
    private RepairHistoryProvider myRepairHistoryProvider;

    @Mock
    private TableRepairMetrics myTableRepairMetrics;

    private RepairStateImpl myRepairState;

    private Set<TokenRange> myTokenRanges = new HashSet<>();

    private Set<LongTokenRange> myConvertedTokenRanges;

    private MockedClock myClock = new MockedClock();

    private final TableReference myTableReference = new TableReference(keyspaceName, tableName);

    @Before
    public void setup() throws Exception
    {
        LOCAL_HOST = InetAddress.getLocalHost();

        myTokenRanges.add(TokenUtil.getRange(0, 1));
        myTokenRanges.add(TokenUtil.getRange(2, 3));

        myConvertedTokenRanges = convert(myTokenRanges);

        doReturn(myKeyspaceMetadata).when(myMetadata).getKeyspace(eq(keyspaceName));

        doReturn(myTokenRanges).when(myMetadata).getTokenRanges(eq(keyspaceName), eq(myHost));

        doReturn("dc1").when(myHost).getDatacenter();
        doReturn("dc1").when(mySecondHost).getDatacenter();
        doReturn("dc1").when(myThirdHost).getDatacenter();

        doReturn(true).when(myHostStates).isUp(any(Host.class));
        doReturn(Sets.newHashSet(myHost)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(Sets.newHashSet(myHost)).when(myMetadata).getAllHosts();

        doReturn(Iterators.emptyIterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(Iterators.emptyIterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        doReturn(LOCAL_HOST).when(myHost).getBroadcastAddress();

        myRepairState = new RepairStateImpl.Builder()
                .withTableReference(myTableReference)
                .withMetadata(myMetadata)
                .withHost(myHost)
                .withHostStates(myHostStates)
                .withRepairHistoryProvider(myRepairHistoryProvider)
                .withRunInterval(RUN_INTERVAL_IN_MS, TimeUnit.MILLISECONDS)
                .withRepairMetrics(myTableRepairMetrics)
                .build();

        myRepairState.setClock(myClock);
    }

    @After
    public void finalVerification()
    {
        verifyNoMoreInteractions(ignoreStubs(myMetadata));
        verifyNoMoreInteractions(ignoreStubs(myHost));
        verifyNoMoreInteractions(ignoreStubs(mySecondHost));
        verifyNoMoreInteractions(ignoreStubs(myThirdHost));
        verifyNoMoreInteractions(ignoreStubs(myHostStates));
        verifyNoMoreInteractions(ignoreStubs(myRepairHistoryProvider));
        verifyNoMoreInteractions(ignoreStubs(myKeyspaceMetadata));
        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
    }

    @Test
    public void testUnknownRepairState()
    {
        assertThat(myRepairState.lastRepairedAt()).isEqualTo(-1L);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.UnknownState.class);
    }

    @Test
    public void testUnknownToRepairedState()
    {
        long expectedRepairedAt = System.currentTimeMillis();

        myClock.setTime(expectedRepairedAt);

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.RepairedState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testUnknownToRepairedWithHistory()
    {
        // setup
        long start = System.currentTimeMillis();
        long notExpectedRepairedAt = start - TimeUnit.HOURS.toMillis(20);
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(15);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, notExpectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, notExpectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.RepairedState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testUnknownToNotRepairedRunnableState()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - FORCE_REPAIR_TIME;
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testNotRepairedRunnableToRepairedState()
    {
        // setup not repaired
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - FORCE_REPAIR_TIME;
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock not repaired
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();

        // setup repaired
        expectedRepairedAt = start - TimeUnit.HOURS.toMillis(12);
        repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock repaired
        myClock.setTime(start + TimeUnit.SECONDS.toMillis(2));
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.RepairedState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testRepairedToNotRepairedRunnableState()
    {
        // setup repaired
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(12);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock repaired
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.RepairedState.class);

        verifyRepairMetrics();

        // setup not repaired
        expectedRepairedAt = start - TimeUnit.DAYS.toMillis(2);
        repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock not repaired
        myClock.setTime(start + TimeUnit.DAYS.toMillis(1));
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testRepairedNotChangingStateIfNoChange()
    {
        // setup repaired
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(12);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        // mock repaired
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        RepairStateImpl.State state = myRepairState.getState();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).isEmpty();
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(state).isInstanceOf(RepairStateImpl.RepairedState.class);

        myClock.setTime(start + TimeUnit.SECONDS.toMillis(2));

        myRepairState.update();

        assertThat(myRepairState.getState()).isEqualTo(state);

        verifyRepairMetrics();
    }

    @Test
    public void testUnknownToNotRepairedNotRunnableStateDownHost()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(25);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        Host downHost = mock(Host.class);
        Set<Host> allHosts = new HashSet<>();
        allHosts.add(myHost);
        allHosts.add(downHost);

        // mock
        myClock.setTime(start);
        doReturn(allHosts).when(myMetadata).getAllHosts();
        doReturn(allHosts).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(false).when(myHostStates).isUp(eq(downHost));

        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).containsExactlyInAnyOrder(new LongTokenRange(0, 1), new LongTokenRange(2, 3));
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedNotRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testRepairedNotRunnableNotChangingRepairedAtWithPartialHistory()
    {
        // setup repaired
        long start = System.currentTimeMillis();
        long checkTime = start + TimeUnit.DAYS.toMillis(1) + 1;
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(12);
        long secondRangeRepairedAt = expectedRepairedAt + 1;

        List<RepairEntry> repairEntriesFirst = new ArrayList<>();
        repairEntriesFirst.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntriesFirst.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, secondRangeRepairedAt));

        List<RepairEntry> repairEntriesPartial = new ArrayList<>();
        repairEntriesPartial.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, secondRangeRepairedAt));

        // mock not repaired
        myClock.setTime(checkTime);
        doReturn(repairEntriesFirst.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();

        // mock not repaired but with partial history
        doReturn(repairEntriesPartial.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), anyLong(), any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testRepairedNotRunnableNotChangingRepairedAtWithEmptyHistory()
    {
        // setup repaired
        long start = System.currentTimeMillis();
        long checkTime = start + TimeUnit.DAYS.toMillis(1) + 1;
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(12);
        long secondRangeRepairedAt = expectedRepairedAt + 1;

        List<RepairEntry> repairEntriesFirst = new ArrayList<>();
        repairEntriesFirst.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntriesFirst.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, secondRangeRepairedAt));

        // mock not repaired
        myClock.setTime(checkTime);
        doReturn(repairEntriesFirst.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();

        // mock not repaired but with empty history
        doReturn(Sets.newHashSet().iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), anyLong(), any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testUnknownToNotRepairedRunnableStateDownHost()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.DAYS.toMillis(2);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        Host downHost = mock(Host.class);
        Host upHost = mock(Host.class);

        Set<Host> allHosts = new HashSet<>();
        allHosts.add(myHost);
        allHosts.add(downHost);
        allHosts.add(upHost);

        Set<Host> expectedReplicas = new HashSet<>();
        expectedReplicas.add(myHost);
        expectedReplicas.add(upHost);

        // mock
        myClock.setTime(start);
        doReturn(allHosts).when(myMetadata).getAllHosts();
        doReturn(allHosts).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(false).when(myHostStates).isUp(eq(downHost));

        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEqualTo(expectedReplicas);
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testFailedRepairRecentlyNotRunnable()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(25);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        long expectedFailedRepairedAt = start - TimeUnit.HOURS.toMillis(1);
        List<RepairEntry> failedRepairEntries = new ArrayList<>();
        failedRepairEntries.add(generateRepairEntry(RepairStatus.FAILED, 0, 1, expectedRepairedAt));
        failedRepairEntries.add(generateRepairEntry(RepairStatus.FAILED, 2, 3, expectedRepairedAt));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(failedRepairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), eq(start), eq(expectedFailedRepairedAt), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).containsExactlyInAnyOrder(new LongTokenRange(0, 1), new LongTokenRange(2, 3));
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedNotRunnableState.class);

        verifyRepairMetrics();
    }

    /**
     * Make sure partial repair does not run in case a repair has been performed recently.
     */
    @Test
    public void testFailedRepairRecentlyNotRunnableForPartial() throws UnknownHostException
    {
        // setup
        Host downHost = mock(Host.class);
        Host upHost = mock(Host.class);
        Set<Host> allHosts = new HashSet<>();
        allHosts.add(myHost);
        allHosts.add(downHost);
        allHosts.add(upHost);

        doReturn(InetAddress.getByName("127.0.0.2")).when(downHost).getBroadcastAddress();
        doReturn(InetAddress.getByName("127.0.0.3")).when(upHost).getBroadcastAddress();

        Set<InetAddress> allAddresses = allHosts.stream().map(Host::getBroadcastAddress).collect(Collectors.toSet());
        Set<InetAddress> partialAddressess = Sets.newHashSet(myHost.getBroadcastAddress(), upHost.getBroadcastAddress());

        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.HOURS.toMillis(49);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt, allAddresses));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt, allAddresses));

        long expectedPartialRepairedAt = start - TimeUnit.HOURS.toMillis(1);
        List<RepairEntry> failedRepairEntries = new ArrayList<>();
        failedRepairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt, partialAddressess));
        failedRepairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt, partialAddressess));

        // mock
        myClock.setTime(start);
        doReturn(allHosts).when(myMetadata).getAllHosts();
        doReturn(allHosts).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(false).when(myHostStates).isUp(eq(downHost));

        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.any());
        doReturn(failedRepairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), eq(start), eq(expectedPartialRepairedAt), Matchers.any());

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getLocalRangesForRepair()).containsExactlyInAnyOrder(new LongTokenRange(0, 1), new LongTokenRange(2, 3));
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedNotRunnableState.class);

        verifyRepairMetrics();
    }

    @Test
    public void testGetReplicasForRepair()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.DAYS.toMillis(1);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        Map<LongTokenRange, Collection<Host>> expectedRangeToReplicas = new HashMap<>();

        expectedRangeToReplicas.put(new LongTokenRange(0, 1), Sets.newHashSet(myHost, host2, host3));
        expectedRangeToReplicas.put(new LongTokenRange(2, 3), Sets.newHashSet(myHost, host2, host3));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getAllHosts();

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        assertThat(myRepairState.getRangeToReplicas()).isEqualTo(expectedRangeToReplicas);

        verifyRepairMetrics();
    }

    @Test
    public void testGetReplicasForPartialRepair()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.DAYS.toMillis(2);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, expectedRepairedAt));

        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        Map<LongTokenRange, Collection<Host>> expectedRangeToReplicas = new HashMap<>();

        expectedRangeToReplicas.put(new LongTokenRange(0, 1), Sets.newHashSet(myHost, host2));
        expectedRangeToReplicas.put(new LongTokenRange(2, 3), Sets.newHashSet(myHost, host2));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(false).when(myHostStates).isUp(eq(host3));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getAllHosts();

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).containsExactlyInAnyOrder(myHost, host2);
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        assertThat(myRepairState.getRangeToReplicas()).isEqualTo(expectedRangeToReplicas);

        verifyRepairMetrics();
    }

    @Test
    public void testGetReplicasForPartialRepairOfOneRange()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.DAYS.toMillis(2);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, start));

        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        Map<LongTokenRange, Collection<Host>> expectedRangeToReplicas = new HashMap<>();

        expectedRangeToReplicas.put(new LongTokenRange(0, 1), Sets.newHashSet(myHost, host2));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(false).when(myHostStates).isUp(eq(host3));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getAllHosts();

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).containsOnly(new LongTokenRange(0, 1));
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).containsExactlyInAnyOrder(myHost, host2);
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        assertThat(myRepairState.getRangeToReplicas()).isEqualTo(expectedRangeToReplicas);

        verifyRepairMetrics();
    }

    @Test
    public void testGetReplicasForRepairOfOneRange()
    {
        // setup
        long start = System.currentTimeMillis();
        long expectedRepairedAt = start - TimeUnit.DAYS.toMillis(2);
        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 0, 1, expectedRepairedAt));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, start));

        Host host2 = mock(Host.class);
        Host host3 = mock(Host.class);

        Map<LongTokenRange, Collection<Host>> expectedRangeToReplicas = new HashMap<>();

        expectedRangeToReplicas.put(new LongTokenRange(0, 1), Sets.newHashSet(myHost, host2, host3));

        // mock
        myClock.setTime(start);
        doReturn(repairEntries.iterator()).when(myRepairHistoryProvider).iterate(eq(myTableReference), anyLong(), Matchers.<Predicate<RepairEntry>> any());
        doReturn(true).when(myHostStates).isUp(eq(host3));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(Sets.newHashSet(myHost, host2, host3)).when(myMetadata).getAllHosts();

        myRepairState.update();

        assertThat(myRepairState.lastRepairedAt()).isEqualTo(expectedRepairedAt);
        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getLocalRangesForRepair()).containsOnly(new LongTokenRange(0, 1));
        assertThat(myRepairState.getAllRanges()).isEqualTo(myConvertedTokenRanges);
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getState()).isInstanceOf(RepairStateImpl.NotRepairedRunnableState.class);

        assertThat(myRepairState.getRangeToReplicas()).isEqualTo(expectedRangeToReplicas);

        verifyRepairMetrics();
    }

    public void testGetDataCentersSingleDataCenterNotEnoughReplicas() throws Exception
    {
        // mock
        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(TokenUtil.getRange(1, 2));
        ranges.add(TokenUtil.getRange(2, 3));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationMap.put("dc1", "3");

        doReturn(false).when(myHostStates).isUp(eq(mySecondHost));
        doReturn(replicationMap).when(myKeyspaceMetadata).getReplication();

        doReturn(Sets.newHashSet(myHost, mySecondHost)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(ranges).when(myMetadata).getTokenRanges(eq(keyspaceName), eq(myHost));

        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 1, 2, System.currentTimeMillis() - FORCE_REPAIR_TIME));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, System.currentTimeMillis() - FORCE_REPAIR_TIME));

        doReturn(repairEntries.iterator())
                .when(myRepairHistoryProvider).iterate(any(TableReference.class), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getDatacentersForRepair()).isEmpty();
    }

    @Test
    public void testGetDataCentersMultipleDataCentersNotEnoughReplicas() throws Exception
    {
        // mock
        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(TokenUtil.getRange(1, 2));
        ranges.add(TokenUtil.getRange(2, 3));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationMap.put("dc1", "1");
        replicationMap.put("dc2", "1");

        doReturn("dc1").when(myHost).getDatacenter();
        doReturn("dc2").when(mySecondHost).getDatacenter();

        doReturn(false).when(myHostStates).isUp(eq(mySecondHost));
        doReturn(replicationMap).when(myKeyspaceMetadata).getReplication();

        doReturn(Sets.newHashSet(myHost, mySecondHost)).when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(ranges).when(myMetadata).getTokenRanges(eq(keyspaceName), eq(myHost));

        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 1, 2, System.currentTimeMillis() - FORCE_REPAIR_TIME));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, System.currentTimeMillis() - FORCE_REPAIR_TIME));

        doReturn(repairEntries.iterator())
                .when(myRepairHistoryProvider).iterate(any(TableReference.class), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.canRepair()).isFalse();
        assertThat(myRepairState.getDatacentersForRepair()).isEmpty();

        verifyRepairMetrics();
    }

    @Test
    public void testGetDatacentersMultipleDataCentersEnoughReplicasInOne() throws Exception
    {
        // mock
        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(TokenUtil.getRange(1, 2));
        ranges.add(TokenUtil.getRange(2, 3));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationMap.put("dc1", "3");
        replicationMap.put("dc2", "3");

        doReturn(replicationMap).when(myKeyspaceMetadata).getReplication();

        doReturn("dc2").when(myThirdHost).getDatacenter();

        doReturn(false).when(myHostStates).isUp(eq(myThirdHost));

        doReturn(Sets.newHashSet(myHost, mySecondHost, myThirdHost))
                .when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(ranges).when(myMetadata).getTokenRanges(eq(keyspaceName), eq(myHost));

        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 1, 2, System.currentTimeMillis() - FORCE_REPAIR_TIME));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, System.currentTimeMillis() - FORCE_REPAIR_TIME));

        doReturn(repairEntries.iterator())
                .when(myRepairHistoryProvider).iterate(any(TableReference.class), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getReplicas()).containsExactlyInAnyOrder(myHost, mySecondHost);
        assertThat(myRepairState.getDatacentersForRepair()).containsExactly("dc1");

        verifyRepairMetrics();
    }

    @Test
    public void testGetDatacentersMultipleDataCentersEnoughReplicas() throws Exception
    {
        // mock
        Set<TokenRange> ranges = new HashSet<>();
        ranges.add(TokenUtil.getRange(1, 2));
        ranges.add(TokenUtil.getRange(2, 3));

        Map<String, String> replicationMap = new HashMap<>();
        replicationMap.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");
        replicationMap.put("dc1", "3");
        replicationMap.put("dc2", "1");

        doReturn(replicationMap).when(myKeyspaceMetadata).getReplication();

        doReturn("dc2").when(myThirdHost).getDatacenter();

        doReturn(replicationMap).when(myKeyspaceMetadata).getReplication();

        doReturn(Sets.newHashSet(myHost, mySecondHost, myThirdHost))
                .when(myMetadata).getReplicas(eq(keyspaceName), any(TokenRange.class));
        doReturn(ranges).when(myMetadata).getTokenRanges(eq(keyspaceName), eq(myHost));

        List<RepairEntry> repairEntries = new ArrayList<>();
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 1, 2, System.currentTimeMillis() - FORCE_REPAIR_TIME));
        repairEntries.add(generateRepairEntry(RepairStatus.SUCCESS, 2, 3, System.currentTimeMillis() - FORCE_REPAIR_TIME));

        doReturn(repairEntries.iterator())
                .when(myRepairHistoryProvider).iterate(any(TableReference.class), anyLong(), Matchers.<Predicate<RepairEntry>> any());

        myRepairState.update();

        assertThat(myRepairState.canRepair()).isTrue();
        assertThat(myRepairState.getReplicas()).isEmpty();
        assertThat(myRepairState.getDatacentersForRepair()).containsExactlyInAnyOrder("dc1", "dc2");

        verifyRepairMetrics();
    }

    private void verifyRepairMetrics()
    {
        int expectedNotRepairedRanges = myRepairState.getLocalRangesForRepair().size();
        int expectedRepairedRanges = myRepairState.getAllRanges().size() - expectedNotRepairedRanges;
        verify(myTableRepairMetrics).repairState(TABLE_REFERENCE, expectedRepairedRanges, expectedNotRepairedRanges);
        verify(myTableRepairMetrics).lastRepairedAt(TABLE_REFERENCE, myRepairState.lastRepairedAt());

        verifyNoMoreInteractions(ignoreStubs(myTableRepairMetrics));
        reset(myTableRepairMetrics);
    }

    private Set<LongTokenRange> convert(Set<TokenRange> tokenRanges)
    {
        Set<LongTokenRange> convertedTokenRanges = new HashSet<>();

        for (TokenRange range : tokenRanges)
        {
            convertedTokenRanges.add(new LongTokenRange((Long) range.getStart().getValue(), (Long) range.getEnd().getValue()));
        }

        return convertedTokenRanges;
    }

    private RepairEntry generateRepairEntry(RepairStatus status, long rangeBegin, long rangeEnd, long startedAt)
    {
        return generateRepairEntry(status, rangeBegin, rangeEnd, startedAt, Sets.newHashSet(LOCAL_HOST));
    }

    private RepairEntry generateRepairEntry(RepairStatus status, long rangeBegin, long rangeEnd, long startedAt, Set<InetAddress> participants)
    {
        return new RepairEntry(new LongTokenRange(rangeBegin, rangeEnd), startedAt, participants, status.toString());
    }

}
