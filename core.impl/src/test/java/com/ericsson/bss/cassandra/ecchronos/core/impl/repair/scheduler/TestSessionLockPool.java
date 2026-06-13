/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.scheduler;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairLockFactoryImpl;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduledTask;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestSessionLockPool
{
    private static final int TEST_PRIORITY = 1;

    private LockFactory myLockFactory;
    private RepairLockFactoryImpl myRepairLockFactory;
    private LockFactory.DistributedLock myDistributedLock;
    private UUID myNodeId;
    private SessionLockPool mySessionLockPool;

    @Before
    public void startup()
    {
        myLockFactory = mock(LockFactory.class);
        myRepairLockFactory = mock(RepairLockFactoryImpl.class);
        myDistributedLock = mock(LockFactory.DistributedLock.class);
        myNodeId = UUID.randomUUID();
        mySessionLockPool = new SessionLockPool(myLockFactory, myRepairLockFactory, myNodeId);
    }

    @Test
    public void testTaskWithoutResourcesDoesNotAcquireLock() throws LockException
    {
        ScheduledTask task = createTask(Collections.emptySet());

        mySessionLockPool.acquireForTask(task);

        verifyNoInteractions(myRepairLockFactory);
    }

    @Test
    public void testSameResourceIsAcquiredOnlyOnce() throws LockException
    {
        RepairResource resource = new RepairResource("dc1", "nodeA");
        ScheduledTask firstTask = createTask(Set.of(resource));
        ScheduledTask secondTask = createTask(Set.of(resource));

        when(myRepairLockFactory.getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId)))
                .thenReturn(myDistributedLock);

        mySessionLockPool.acquireForTask(firstTask);
        mySessionLockPool.acquireForTask(secondTask);

        verify(myRepairLockFactory, times(1)).getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId));
    }

    @Test
    public void testOnlyMissingResourcesAreAcquired() throws LockException
    {
        RepairResource resourceA = new RepairResource("dc1", "nodeA");
        RepairResource resourceB = new RepairResource("dc1", "nodeB");
        RepairResource resourceC = new RepairResource("dc1", "nodeC");

        ScheduledTask firstTask = createTask(Set.of(resourceA, resourceB));
        ScheduledTask secondTask = createTask(Set.of(resourceB, resourceC));

        LockFactory.DistributedLock firstLock = mock(LockFactory.DistributedLock.class);
        LockFactory.DistributedLock secondLock = mock(LockFactory.DistributedLock.class);

        when(myRepairLockFactory.getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId)))
                .thenReturn(firstLock, secondLock);

        mySessionLockPool.acquireForTask(firstTask);
        mySessionLockPool.acquireForTask(secondTask);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Set<RepairResource>> resourceCaptor =
                ArgumentCaptor.forClass(Set.class);

        verify(myRepairLockFactory, times(2)).getLock(
                eq(myLockFactory),
                resourceCaptor.capture(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId));

        org.assertj.core.api.Assertions.assertThat(resourceCaptor.getAllValues().get(0))
                .containsExactlyInAnyOrder(resourceA, resourceB);
        org.assertj.core.api.Assertions.assertThat(resourceCaptor.getAllValues().get(1))
                .containsExactly(resourceC);
    }

    @Test
    public void testFailedAcquisitionIsRetried() throws LockException
    {
        RepairResource resource = new RepairResource("dc1", "nodeA");
        ScheduledTask task = createTask(Set.of(resource));

        when(myRepairLockFactory.getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId)))
                .thenThrow(new LockException("Resource busy"))
                .thenReturn(myDistributedLock);

        assertThatThrownBy(() -> mySessionLockPool.acquireForTask(task))
                .isInstanceOf(LockException.class)
                .hasMessage("Resource busy");

        mySessionLockPool.acquireForTask(task);

        verify(myRepairLockFactory, times(2)).getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId));
    }

    @Test
    public void testCloseReleasesAllAcquiredLocks() throws Exception
    {
        RepairResource resourceA = new RepairResource("dc1", "nodeA");
        RepairResource resourceB = new RepairResource("dc1", "nodeB");

        ScheduledTask firstTask = createTask(Set.of(resourceA));
        ScheduledTask secondTask = createTask(Set.of(resourceB));

        LockFactory.DistributedLock firstLock = mock(LockFactory.DistributedLock.class);
        LockFactory.DistributedLock secondLock = mock(LockFactory.DistributedLock.class);

        when(myRepairLockFactory.getLock(
                eq(myLockFactory),
                any(),
                any(),
                eq(TEST_PRIORITY),
                eq(myNodeId)))
                .thenReturn(firstLock, secondLock);

        mySessionLockPool.acquireForTask(firstTask);
        mySessionLockPool.acquireForTask(secondTask);

        mySessionLockPool.close();

        verify(firstLock).close();
        verify(secondLock).close();
    }

    private ScheduledTask createTask(final Set<RepairResource> resources)
    {
        ScheduledTask task = mock(ScheduledTask.class);
        when(task.getRepairResources()).thenReturn(resources);
        when(task.getLockMetadata()).thenReturn(Map.of("test", "session-lock-pool"));
        when(task.getPriority()).thenReturn(TEST_PRIORITY);
        return task;
    }
}
