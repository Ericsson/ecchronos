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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.datastax.driver.core.UDTValue;
import com.ericsson.bss.cassandra.ecchronos.core.repair.OngoingJob.Status;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestOngoingJob
{
    private static final String keyspaceName = "keyspace";
    private static final String tableName = "table";

    private final Map<LongTokenRange, ImmutableSet<Node>> myTokenMap = new HashMap<>();

    @Mock
    private OnDemandStatus myOnDemandStatus;

    @Mock
    private ReplicationState myReplicationState;

    @Mock
    private UDTValue myUdtValue;

    @Captor
    private ArgumentCaptor<Set<UDTValue>> myUdtSetCaptor;

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
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForRestartedJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        when(myOnDemandStatus.getStartTokenFrom(myUdtValue)).thenReturn(-50L);
        when(myOnDemandStatus.getEndTokenFrom(myUdtValue)).thenReturn(700L);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), repiaredTokens, Status.started, null)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.started);
        assertThat(ongoingJob.getCompletedTime()).isEqualTo(-1);
    }

    @Test
    public void testOngoingJobForFinishedJobIsCreated()
    {
        UUID jobId = UUID.randomUUID();
        Set<LongTokenRange> expectedRepairedTokens = new HashSet<>();
        expectedRepairedTokens.add(new LongTokenRange(-50L, 700L));
        Set<UDTValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        when(myOnDemandStatus.getStartTokenFrom(myUdtValue)).thenReturn(-50L);
        when(myOnDemandStatus.getEndTokenFrom(myUdtValue)).thenReturn(700L);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.hashCode(), repiaredTokens, Status.finished, 12345L)
                .build();

        assertThat(ongoingJob.getJobId()).isEqualTo(jobId);
        assertThat(ongoingJob.getRepairedTokens()).isEqualTo(expectedRepairedTokens);
        assertThat(ongoingJob.getTableReference()).isEqualTo(myTableReference);
        assertThat(ongoingJob.getTokens()).isEqualTo(myTokenMap);
        assertThat(ongoingJob.getStatus()).isEqualTo(Status.finished);
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
        Set<UDTValue> rangeSet = myUdtSetCaptor.getValue();
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
        Set<UDTValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.keySet().hashCode(), repiaredTokens, Status.started, null)
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
        Set<UDTValue> repiaredTokens = new HashSet<>();
        repiaredTokens.add(myUdtValue);

        OngoingJob ongoingJob = new OngoingJob.Builder()
                .withOnDemandStatus(myOnDemandStatus)
                .withReplicationState(myReplicationState)
                .withTableReference(myTableReference)
                .withOngoingJobInfo(jobId, myTokenMap.keySet().hashCode() - 1, repiaredTokens, Status.started, null)
                .build();

        boolean result = ongoingJob.hasTopologyChanged();

        assertThat(result).isTrue();
    }
}
