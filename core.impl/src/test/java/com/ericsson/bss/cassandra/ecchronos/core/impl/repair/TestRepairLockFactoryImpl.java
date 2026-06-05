/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairLockFactoryImpl
{
    private static final int LOCKS_PER_RESOURCE = 3;
    private static final RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();

    @Mock
    private LockFactory mockLockFactory;

    @Mock
    private LockFactory.DistributedLock mockLock;

    @BeforeClass
    public static void init()
    {
        RepairLockFactoryImpl.configure(LOCKS_PER_RESOURCE);
    }

    @Before
    public void setup()
    {
        RepairLockFactoryImpl.resetLocalGates();
        when(mockLockFactory.getCachedFailure(any(UUID.class), anyString(), anyString())).thenReturn(Optional.empty());
    }

    @Test
    public void testNothingToLockThrowsException()
    {
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata);
        verify(mockLock, never()).close();
    }

    @Test
    public void testSingleLock() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSuccessfulLocking(repairResource, priority, metadata);

        verifyLocksAreTriedWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
        verify(mockLock, never()).close();
    }

    @Test
    public void testSingleLockNotSufficientNodes() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulLocking(repairResource, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
        verify(mockLock, never()).close();
    }

    @Test
    public void testSingleLockFailing() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulLocking(repairResource, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
        verify(mockLock, never()).close();
    }

    @Test
    public void testUnexpectedException() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResource2 = new RepairResource("DC2", "my-resource-dc2");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSuccessfulLocking(repairResource, priority, metadata);
        withUnexpectedLockingFailure(repairResource2, priority, metadata, NullPointerException.class);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, NullPointerException.class, repairResource, repairResource2);
        verify(mockLock).close();
    }

    @Test
    public void testMultipleLocks() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSuccessfulLocking(repairResourceDc1, priority, metadata);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyLocksAreTriedWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verify(mockLock, never()).close();
    }

    @Test
    public void testMultipleLocksOneFailing() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSuccessfulLocking(repairResourceDc1, priority, metadata);
        withUnsuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verify(mockLock).close();
    }

    @Test
    public void testMultipleLocksOneHasCachedFailure() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulCachedLock(repairResourceDc1);

        withSuccessfulLocking(repairResourceDc1, priority, metadata);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verifyNoLockWasTried();
        verify(mockLock, never()).close();
    }

    @Test
    public void testMultipleLocksTheOtherHasCachedFailure() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulCachedLock(repairResourceDc2);

        withSuccessfulLocking(repairResourceDc1, priority, metadata);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verifyNoLockWasTried();
        verify(mockLock, never()).close();
    }

    @Test
    public void testGetLocksPerResourceReturnsConfiguredValue()
    {
        assertThat(RepairLockFactoryImpl.getLocksPerResource()).isEqualTo(LOCKS_PER_RESOURCE);
    }

    @Test
    public void testRuntimeReconfiguration()
    {
        try
        {
            RepairLockFactoryImpl.configure(5);
            assertThat(RepairLockFactoryImpl.getLocksPerResource()).isEqualTo(5);
        }
        finally
        {
            RepairLockFactoryImpl.configure(LOCKS_PER_RESOURCE);
        }
    }

    @Test
    public void testConfigureWithInvalidValueThrows()
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> RepairLockFactoryImpl.configure(0));
    }

    @Test
    public void testLocalGateBlocksConcurrentAccessToSameSlot() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "gate-test");
        Map<String, String> metadata = Collections.singletonMap("key", "value");
        int priority = 1;

        for (int slot = 1; slot <= LOCKS_PER_RESOURCE; slot++)
        {
            when(mockLockFactory.tryLock(eq("DC1"), eq(repairResource.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenReturn(mockLock);
        }

        // Acquire all 3 slots
        LockFactory.DistributedLock lock1 = repairLockFactory.getLock(
                mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID());
        LockFactory.DistributedLock lock2 = repairLockFactory.getLock(
                mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID());
        LockFactory.DistributedLock lock3 = repairLockFactory.getLock(
                mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID());

        // Fourth acquire fails - all local gates held
        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(
                        mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID()));

        // Release one - next acquire succeeds
        lock1.close();
        LockFactory.DistributedLock lock4 = repairLockFactory.getLock(
                mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID());
        assertThat(lock4).isNotNull();

        lock2.close();
        lock3.close();
        lock4.close();
    }

    @Test
    public void testLocalGateReleasedOnCASFailure() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "cas-fail-test");
        Map<String, String> metadata = Collections.singletonMap("key", "value");
        int priority = 1;

        for (int slot = 1; slot <= LOCKS_PER_RESOURCE; slot++)
        {
            when(mockLockFactory.tryLock(eq("DC1"), eq(repairResource.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenThrow(new LockException("CAS failed"));
        }

        // First attempt fails, gates released
        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(
                        mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID()));

        // Second attempt also tries CAS (gates were released)
        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(
                        mockLockFactory, Sets.newHashSet(repairResource), metadata, priority, UUID.randomUUID()));

        // 6 CAS attempts total (3 slots × 2 rounds)
        verify(mockLockFactory, times(6)).tryLock(anyString(), anyString(), anyInt(), anyMap(), any());
    }

    @Test
    public void testResourceGateBlocksOverlappingThreads() throws Exception
    {
        RepairResource shared = new RepairResource("DC1", "shared-res");
        Map<String, String> metadata = Collections.singletonMap("key", "value");
        int priority = 1;

        java.util.concurrent.CountDownLatch t1InCAS = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch t1Proceed = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch t2Started = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch t2Acquired = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean t2WasBlocked = new java.util.concurrent.atomic.AtomicBoolean(false);

        // t1's CAS blocks until we signal it to proceed
        for (int slot = 1; slot <= LOCKS_PER_RESOURCE; slot++)
        {
            when(mockLockFactory.tryLock(eq("DC1"), eq(shared.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenAnswer(invocation -> {
                        t1InCAS.countDown(); // Signal t1 is inside CAS (holding resource gate)
                        t1Proceed.await();   // Wait until test signals to proceed
                        return mockLock;
                    });
        }

        Thread t1 = new Thread(() -> {
            try
            {
                repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(shared), metadata, priority, UUID.randomUUID());
            }
            catch (Exception ignored) { }
        });

        Thread t2 = new Thread(() -> {
            try
            {
                t2Started.countDown();
                repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(shared), metadata, priority, UUID.randomUUID());
            }
            catch (Exception ignored) { }
            t2Acquired.countDown();
        });

        t1.start();
        t1InCAS.await(); // t1 is now inside CAS, holding the resource gate

        t2.start();
        t2Started.await();
        Thread.sleep(20); // Give t2 time to hit the resource gate

        // t2 should be blocked (not yet acquired)
        t2WasBlocked.set(t2Acquired.getCount() == 1);

        // Release t1
        t1Proceed.countDown();

        // t2 should now complete
        assertThat(t2Acquired.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(t2WasBlocked.get()).isTrue();

        t1.join(2000);
        t2.join(2000);
    }

    @Test
    public void testResourceGateAllowsNonOverlappingThreads() throws Exception
    {
        RepairResource res1 = new RepairResource("DC1", "disjoint-a");
        RepairResource res2 = new RepairResource("DC1", "disjoint-b");
        Map<String, String> metadata = Collections.singletonMap("key", "value");
        int priority = 1;

        java.util.concurrent.CountDownLatch bothInCAS = new java.util.concurrent.CountDownLatch(2);
        java.util.concurrent.CountDownLatch proceed = new java.util.concurrent.CountDownLatch(1);

        // Both resources' CAS blocks until signaled — if they're serialized, we'd deadlock
        for (int slot = 1; slot <= LOCKS_PER_RESOURCE; slot++)
        {
            when(mockLockFactory.tryLock(eq("DC1"), eq(res1.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenAnswer(invocation -> {
                        bothInCAS.countDown();
                        proceed.await();
                        return mockLock;
                    });
            when(mockLockFactory.tryLock(eq("DC1"), eq(res2.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenAnswer(invocation -> {
                        bothInCAS.countDown();
                        proceed.await();
                        return mockLock;
                    });
        }

        Thread t1 = new Thread(() -> {
            try { repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(res1), metadata, priority, UUID.randomUUID()); }
            catch (Exception ignored) { }
        });
        Thread t2 = new Thread(() -> {
            try { repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(res2), metadata, priority, UUID.randomUUID()); }
            catch (Exception ignored) { }
        });

        t1.start();
        t2.start();

        // Both threads must reach CAS concurrently (non-overlapping resources = no blocking)
        assertThat(bothInCAS.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        proceed.countDown();
        t1.join(2000);
        t2.join(2000);
    }

    @Test
    public void testResourceGateReleasedOnLockFailure() throws Exception
    {
        RepairResource resource = new RepairResource("DC1", "fail-release");
        Map<String, String> metadata = Collections.singletonMap("key", "value");
        int priority = 1;

        for (int slot = 1; slot <= LOCKS_PER_RESOURCE; slot++)
        {
            when(mockLockFactory.tryLock(eq("DC1"), eq(resource.getResourceName(slot)), eq(priority), eq(metadata), any()))
                    .thenThrow(new LockException("fail"));
        }

        // First attempt fails
        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(
                        mockLockFactory, Sets.newHashSet(resource), metadata, priority, UUID.randomUUID()));

        // Resource gate must be released - second attempt should not deadlock
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
        Thread t = new Thread(() -> {
            try
            {
                repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(resource), metadata, priority, UUID.randomUUID());
            }
            catch (LockException ignored) { }
            done.countDown();
        });
        t.start();
        assertThat(done.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    }

    private void verifyNoLockWasTried() throws LockException
    {
        verify(mockLockFactory, never()).tryLock(anyString(), anyString(), anyInt(), anyMap(), any());
    }

    private void verifyLocksAreTriedWhenGettingLock(RepairLockFactory repairLockFactory, int priority, Map<String, String> metadata, RepairResource... repairResources) throws LockException
    {
        repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResources), metadata, priority, UUID.randomUUID());

        for (RepairResource repairResource : repairResources)
        {
            verify(mockLockFactory).tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata), any());
        }
    }

    private void verifyExceptionIsThrownWhenGettingLock(RepairLockFactory repairLockFactory, int priority, Map<String, String> metadata, RepairResource... repairResources)
    {
        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, LockException.class, repairResources);
    }

    private void verifyExceptionIsThrownWhenGettingLock(RepairLockFactory repairLockFactory, int priority, Map<String, String> metadata, Class exceptionType, RepairResource... repairResources)
    {
        assertThatExceptionOfType(exceptionType)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newLinkedHashSet(Arrays.asList(repairResources)), metadata, priority, UUID.randomUUID()));
    }

    private void withUnsuccessfulCachedLock(RepairResource repairResource)
    {
        when(mockLockFactory.getCachedFailure(any(UUID.class), eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)))).thenReturn(Optional.of(new LockException("")));
    }

    private void withSuccessfulLocking(RepairResource repairResource, int priority, Map<String, String> metadata) throws LockException
    {
        when(mockLockFactory.tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata), any())).thenReturn(mockLock);
    }

    private void withUnsuccessfulLocking(RepairResource repairResource, int priority, Map<String, String> metadata) throws LockException
    {
        when(mockLockFactory.tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata), any())).thenThrow(new LockException(""));
    }

    private void withUnexpectedLockingFailure(RepairResource repairResource, int priority, Map<String, String> metadata, Class exceptionClass) throws LockException
    {
        when(mockLockFactory.tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata), any())).thenThrow(exceptionClass);
    }

}
