/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import static com.ericsson.bss.cassandra.ecchronos.core.MockTableReferenceFactory.tableReference;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.data.UdtValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestOngoingJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private final Map<LongTokenRange, ImmutableSet<DriverNode>> myTokenMap = new HashMap<>();

    @Mock
    private OnDemandStatus myOnDemandStatus;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private UdtValue myUdtValue;

    @Captor
    private ArgumentCaptor<Set<UdtValue>> myUdtSetCaptor;

    private final TableReference myTableReference = tableReference(keyspaceName, tableName);

    @Before
    public void setup()
    {
        when(myReplicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(myTokenMap);
    }

    @Test
    public void testOngoingJobForNewJobIsCreated()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        assertThat(ongoingJob.getJobId()).isNotNull();
        assertThat(ongoingJob.getRepairedTokens()).isEmpty();
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForNewIncrementalJobIsCreated()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withRepairType(RepairOptions.RepairType.INCREMENTAL)
                .build();

        assertThat(ongoingJob.getJobId()).isNotNull();
        assertThat(ongoingJob.getRepairedTokens()).isEmpty();
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.INCREMENTAL);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForRestartedJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UdtValue> repairedTokens = new HashSet<>();
        repairedTokens.add(myUdtValue);

        when(myOnDemandStatus.getStartTokenFrom(myUdtValue)).thenReturn(-50L);
        when(myOnDemandStatus.getEndTokenFrom(myUdtValue)).thenReturn(700L);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), repairedTokens, Status.started, null, RepairOptions.RepairType.VNODE)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForRestartedIncrementalJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), Collections.emptySet(), Status.started, null, RepairOptions.RepairType.INCREMENTAL)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(Collections.emptySet());
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.INCREMENTAL);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForFinishedJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UdtValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        when(myOnDemandStatus.getStartTokenFrom(myUdtValue)).thenReturn(-50L);
        when(myOnDemandStatus.getEndTokenFrom(myUdtValue)).thenReturn(700L);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), repiaredTokens, Status.finished, 12345L, RepairOptions.RepairType.VNODE)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.finished);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(12345L);
    }

    @Test
    public void testOngoingJobForFinishedJobWithoutRepairTypeIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UdtValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        when(myOnDemandStatus.getStartTokenFrom(myUdtValue)).thenReturn(-50L);
        when(myOnDemandStatus.getEndTokenFrom(myUdtValue)).thenReturn(700L);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), repiaredTokens, Status.finished, 12345L, null)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.finished);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.VNODE);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(12345L);
    }

    @Test
    public void testOngoingJobForFinishedIncrementalJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), Collections.emptySet(), Status.finished, 12345L, RepairOptions.RepairType.INCREMENTAL)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(Collections.emptySet());
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.finished);
        assertThat(ongoingJob.getRepairType()).isEqualTo(RepairOptions.RepairType.INCREMENTAL);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(12345L);
    }

    @Test
    public void testFinishRange()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        Set<LongTokenRange> finishedRanges = new HashSet<>();
        finishedRanges.add(new LongTokenRange(-50L, 700L));
        when(myOnDemandStatus.createUDTTokenRangeValue(-50L, 700L)).thenReturn(myUdtValue);
        ongoingJob.finishRanges(finishedRanges);

        verify(myOnDemandStatus).updateJob(any(UUID.class), myUdtSetCaptor.capture());
        Set<UdtValue> rangeSet = myUdtSetCaptor.getValue();
        assertThat(rangeSet).containsOnly(myUdtValue);
    }

    @Test
    public void testFinishJob()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        ongoingJob.finishJob();

        verify(myOnDemandStatus).finishJob(any(UUID.class));
    }

    @Test
    public void testFailJob()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        ongoingJob.failJob();

        verify(myOnDemandStatus).failJob(any(UUID.class));
    }

    @Test
    public void testHasTopologyChangedWithSameTopology()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        boolean result = ongoingJob.hasTopologyChanged();

        assertThat(result).isFalse();
    }

    @Test
    public void testHasTopologyChangedWithChangedTopology()
    {
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .build();

        when(myReplicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(null);

        boolean result = ongoingJob.hasTopologyChanged();

        assertThat(result).isTrue();
    }

    @Test
    public void testHasTopologyChangedWithSameTopologyAfterRestart()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UdtValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.keySet().hashCode(), repiaredTokens, Status.started, null, null)
                .build();

        boolean result = ongoingJob.hasTopologyChanged();

        assertThat(result).isFalse();
    }

    @Test
    public void testHasTopologyChangedWithChangedTopologyAfterRestart()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UdtValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.keySet().hashCode() - 1, repiaredTokens, Status.started, null, null)
                .build();

        boolean result = ongoingJob.hasTopologyChanged();

        assertThat(result).isTrue();
    }

    @Test
    public void testStartClusterWideJob()
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> thisNodeTokenMap = new HashMap<>();
        DriverNode node1 = mock(DriverNode.class);
        DriverNode node2 = mock(DriverNode.class);
        DriverNode node3 = mock(DriverNode.class);
        DriverNode node4 = mock(DriverNode.class);

        LongTokenRange range1 = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> range1Replicas = ImmutableSet.of(node1, node2, node3);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        ImmutableSet<DriverNode> range2Replicas = ImmutableSet.of(node1, node2, node3);
        LongTokenRange range3 = new LongTokenRange(3, 4);
        ImmutableSet<DriverNode> range3Replicas = ImmutableSet.of(node2, node1, node3);
        LongTokenRange range4 = new LongTokenRange(4, 5);
        ImmutableSet<DriverNode> range4Replicas = ImmutableSet.of(node2, node1, node3);
        LongTokenRange range5 = new LongTokenRange(5, 6);
        ImmutableSet<DriverNode> range5Replicas = ImmutableSet.of(node3, node2, node1);

        LongTokenRange range6 = new LongTokenRange(6, 7);
        ImmutableSet<DriverNode> range6Replicas = ImmutableSet.of(node2, node3, node4);
        LongTokenRange range7 = new LongTokenRange(7, 8);
        ImmutableSet<DriverNode> range7Replicas = ImmutableSet.of(node2, node3, node4);
        LongTokenRange range8 = new LongTokenRange(8, 9);
        ImmutableSet<DriverNode> range8Replicas = ImmutableSet.of(node3, node2, node4);
        LongTokenRange range9 = new LongTokenRange(9, 10);
        ImmutableSet<DriverNode> range9Replicas = ImmutableSet.of(node4, node3, node2);

        thisNodeTokenMap.put(range1, range1Replicas);
        thisNodeTokenMap.put(range2, range2Replicas);
        thisNodeTokenMap.put(range3, range3Replicas);
        thisNodeTokenMap.put(range4, range4Replicas);
        thisNodeTokenMap.put(range5, range5Replicas);

        Map<LongTokenRange, ImmutableSet<DriverNode>> allTokenMap = new HashMap<>();
        allTokenMap.put(range1, range1Replicas);
        allTokenMap.put(range2, range2Replicas);
        allTokenMap.put(range3, range3Replicas);
        allTokenMap.put(range4, range4Replicas);
        allTokenMap.put(range5, range5Replicas);
        allTokenMap.put(range6, range6Replicas);
        allTokenMap.put(range7, range7Replicas);
        allTokenMap.put(range8, range8Replicas);
        allTokenMap.put(range9, range9Replicas);
        ReplicationState replicationState = mock(ReplicationState.class);
        when(replicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(thisNodeTokenMap);
        when(replicationState.getTokenRanges(myTableReference)).thenReturn(allTokenMap);
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(replicationState)
                .withTableReference(myTableReference)
                .build();
        UUID jobId = ongoingJob.getJobId();
        verify(myOnDemandStatus).addNewJob(jobId, myTableReference, ongoingJob.getTokens().keySet().hashCode(), RepairOptions.RepairType.VNODE);

        ongoingJob.startClusterWideJob(RepairOptions.RepairType.VNODE);

        Set<LongTokenRange> repairedRangesNode2 = new HashSet<>();
        //Node2 will repair range6 and range7
        repairedRangesNode2.add(range1);
        repairedRangesNode2.add(range2);
        repairedRangesNode2.add(range3);
        repairedRangesNode2.add(range4);
        repairedRangesNode2.add(range5);
        repairedRangesNode2.add(range8);
        repairedRangesNode2.add(range9);
        verify(myOnDemandStatus).addNewJob(node2.getId(), jobId, myTableReference, allTokenMap.keySet().hashCode(), repairedRangesNode2, RepairOptions.RepairType.VNODE);

        Set<LongTokenRange> repairedRangesNode3 = new HashSet<>();
        //Node3 will repair range8
        repairedRangesNode3.add(range1);
        repairedRangesNode3.add(range2);
        repairedRangesNode3.add(range3);
        repairedRangesNode3.add(range4);
        repairedRangesNode3.add(range5);
        repairedRangesNode3.add(range6);
        repairedRangesNode3.add(range7);
        repairedRangesNode3.add(range9);
        verify(myOnDemandStatus).addNewJob(node3.getId(), jobId, myTableReference, allTokenMap.keySet().hashCode(), repairedRangesNode3, RepairOptions.RepairType.VNODE);

        Set<LongTokenRange> repairedRangesNode4 = new HashSet<>();
        //Node4 will repair range9, ranges range1-range5 shouldn't be here since node4 is not replica for those
        repairedRangesNode4.add(range6);
        repairedRangesNode4.add(range7);
        repairedRangesNode4.add(range8);
        //Node4 is replica to node2 and node3
        Map<LongTokenRange, ImmutableSet<DriverNode>> node4TokenMap = new HashMap<>();
        node4TokenMap.put(range6, range6Replicas);
        node4TokenMap.put(range7, range7Replicas);
        node4TokenMap.put(range8, range8Replicas);
        node4TokenMap.put(range9, range9Replicas);
        verify(myOnDemandStatus).addNewJob(node4.getId(), jobId, myTableReference, node4TokenMap.keySet().hashCode(), repairedRangesNode4, RepairOptions.RepairType.VNODE);
        verifyNoMoreInteractions(myOnDemandStatus);
    }

    @Test
    public void testStartIncrementalClusterWideJob()
    {
        Map<LongTokenRange, ImmutableSet<DriverNode>> thisNodeTokenMap = new HashMap<>();
        DriverNode node1 = mock(DriverNode.class);
        DriverNode node2 = mock(DriverNode.class);
        UUID node2Id = UUID.randomUUID();
        when(node2.getId()).thenReturn(node2Id);
        DriverNode node3 = mock(DriverNode.class);
        UUID node3Id = UUID.randomUUID();
        when(node3.getId()).thenReturn(node3Id);
        DriverNode node4 = mock(DriverNode.class);
        UUID node4Id = UUID.randomUUID();
        when(node4.getId()).thenReturn(node4Id);

        LongTokenRange range1 = new LongTokenRange(1, 2);
        ImmutableSet<DriverNode> range1Replicas = ImmutableSet.of(node1, node2, node3);
        LongTokenRange range2 = new LongTokenRange(2, 3);
        ImmutableSet<DriverNode> range2Replicas = ImmutableSet.of(node1, node2, node3);
        LongTokenRange range3 = new LongTokenRange(3, 4);
        ImmutableSet<DriverNode> range3Replicas = ImmutableSet.of(node2, node1, node3);
        LongTokenRange range4 = new LongTokenRange(4, 5);
        ImmutableSet<DriverNode> range4Replicas = ImmutableSet.of(node2, node1, node3);
        LongTokenRange range5 = new LongTokenRange(5, 6);
        ImmutableSet<DriverNode> range5Replicas = ImmutableSet.of(node3, node2, node1);

        LongTokenRange range6 = new LongTokenRange(6, 7);
        ImmutableSet<DriverNode> range6Replicas = ImmutableSet.of(node2, node3, node4);
        LongTokenRange range7 = new LongTokenRange(7, 8);
        ImmutableSet<DriverNode> range7Replicas = ImmutableSet.of(node2, node3, node4);
        LongTokenRange range8 = new LongTokenRange(8, 9);
        ImmutableSet<DriverNode> range8Replicas = ImmutableSet.of(node3, node2, node4);
        LongTokenRange range9 = new LongTokenRange(9, 10);
        ImmutableSet<DriverNode> range9Replicas = ImmutableSet.of(node4, node3, node2);

        thisNodeTokenMap.put(range1, range1Replicas);
        thisNodeTokenMap.put(range2, range2Replicas);
        thisNodeTokenMap.put(range3, range3Replicas);
        thisNodeTokenMap.put(range4, range4Replicas);
        thisNodeTokenMap.put(range5, range5Replicas);

        Map<LongTokenRange, ImmutableSet<DriverNode>> allTokenMap = new HashMap<>();
        allTokenMap.put(range1, range1Replicas);
        allTokenMap.put(range2, range2Replicas);
        allTokenMap.put(range3, range3Replicas);
        allTokenMap.put(range4, range4Replicas);
        allTokenMap.put(range5, range5Replicas);
        allTokenMap.put(range6, range6Replicas);
        allTokenMap.put(range7, range7Replicas);
        allTokenMap.put(range8, range8Replicas);
        allTokenMap.put(range9, range9Replicas);
        ReplicationState replicationState = mock(ReplicationState.class);
        when(replicationState.getTokenRangeToReplicas(myTableReference)).thenReturn(thisNodeTokenMap);
        when(replicationState.getTokenRanges(myTableReference)).thenReturn(allTokenMap);
        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(replicationState)
                .withTableReference(myTableReference)
                .withRepairType(RepairOptions.RepairType.INCREMENTAL)
                .build();
        UUID jobId = ongoingJob.getJobId();
        verify(myOnDemandStatus).addNewJob(jobId, myTableReference, ongoingJob.getTokens().keySet().hashCode(), RepairOptions.RepairType.INCREMENTAL);

        ongoingJob.startClusterWideJob(RepairOptions.RepairType.INCREMENTAL);

        verify(myOnDemandStatus).addNewJob(node2Id, jobId, myTableReference, 0, Collections.emptySet(), RepairOptions.RepairType.INCREMENTAL);
        verify(myOnDemandStatus).addNewJob(node3Id, jobId, myTableReference, 0, Collections.emptySet(), RepairOptions.RepairType.INCREMENTAL);
        verify(myOnDemandStatus).addNewJob(node4Id, jobId, myTableReference, 0, Collections.emptySet(), RepairOptions.RepairType.INCREMENTAL);
        verifyNoMoreInteractions(myOnDemandStatus);
    }
}
