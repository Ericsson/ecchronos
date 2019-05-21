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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairLockFactoryImpl
{
    private static final int LOCKS_PER_RESOURCE = 1;

    @Mock
    private LockFactory mockLockFactory;

    @Mock
    private LockFactory.DistributedLock mockLock;

    @Before
    public void setup()
    {
        when(mockLockFactory.getCachedFailure(anyString(), anyString())).thenReturn(Optional.empty());
    }

    @Test
    public void testNothingToLockThrowsException()
    {
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata);
    }

    @Test
    public void testSingleLock() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSufficientNodesForLocking(repairResource);
        withSuccessfulLocking(repairResource, priority, metadata);

        verifyLocksAreTriedWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
    }

    @Test
    public void testSingleLockNotSufficientNodes() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withoutSufficientNodesForLocking(repairResource);
        withSuccessfulLocking(repairResource, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
    }

    @Test
    public void testSingleLockFailing() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSufficientNodesForLocking(repairResource);
        withUnsuccessfulLocking(repairResource, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResource);
    }

    @Test
    public void testMultipleLocks() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSufficientNodesForLocking(repairResourceDc1);
        withSufficientNodesForLocking(repairResourceDc2);

        withSuccessfulLocking(repairResourceDc1, priority, metadata);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyLocksAreTriedWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
    }

    @Test
    public void testMultipleLocksNotSufficientNodes()
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSufficientNodesForLocking(repairResourceDc1);
        withoutSufficientNodesForLocking(repairResourceDc2);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
    }

    @Test
    public void testMultipleLocksOneFailing() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withSufficientNodesForLocking(repairResourceDc1);
        withSuccessfulLocking(repairResourceDc1, priority, metadata);

        withSufficientNodesForLocking(repairResourceDc2);
        withUnsuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
    }

    @Test
    public void testMultipleLocksOneHasCachedFailure() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulCachedLock(repairResourceDc1);

        withSufficientNodesForLocking(repairResourceDc1);
        withSuccessfulLocking(repairResourceDc1, priority, metadata);

        withSufficientNodesForLocking(repairResourceDc2);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verifyNoLockWasTried();
    }

    @Test
    public void testMultipleLocksTheOtherHasCachedFailure() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = Collections.singletonMap("metadatakey", "metadatavalue");
        int priority = 1;

        withUnsuccessfulCachedLock(repairResourceDc2);

        withSufficientNodesForLocking(repairResourceDc1);
        withSuccessfulLocking(repairResourceDc1, priority, metadata);

        withSufficientNodesForLocking(repairResourceDc2);
        withSuccessfulLocking(repairResourceDc2, priority, metadata);

        verifyExceptionIsThrownWhenGettingLock(repairLockFactory, priority, metadata, repairResourceDc1, repairResourceDc2);
        verifyNoLockWasTried();
    }

    private void verifyNoLockWasTried() throws LockException
    {
        verify(mockLockFactory, never()).tryLock(anyString(), anyString(), anyInt(), anyMapOf(String.class, String.class));
    }

    private void verifyLocksAreTriedWhenGettingLock(RepairLockFactory repairLockFactory, int priority, Map<String, String> metadata, RepairResource... repairResources) throws LockException
    {
        try (LockFactory.DistributedLock lock = repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResources), metadata, priority))
        {
            // Nothing to do here
        }

        for (RepairResource repairResource : repairResources)
        {
            verify(mockLockFactory).tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata));
        }
    }

    private void verifyExceptionIsThrownWhenGettingLock(RepairLockFactory repairLockFactory, int priority, Map<String, String> metadata, RepairResource... repairResources)
    {
        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResources), metadata, priority));
    }

    private void withUnsuccessfulCachedLock(RepairResource repairResource)
    {
        when(mockLockFactory.getCachedFailure(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)))).thenReturn(Optional.of(new LockException("")));
    }

    private void withSuccessfulLocking(RepairResource repairResource, int priority, Map<String, String> metadata) throws LockException
    {
        when(mockLockFactory.tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata))).thenReturn(mockLock);
    }

    private void withUnsuccessfulLocking(RepairResource repairResource, int priority, Map<String, String> metadata) throws LockException
    {
        when(mockLockFactory.tryLock(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)), eq(priority), eq(metadata))).thenThrow(new LockException(""));
    }

    private void withSufficientNodesForLocking(RepairResource repairResource)
    {
        when(mockLockFactory.sufficientNodesForLocking(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)))).thenReturn(true);
    }

    private void withoutSufficientNodesForLocking(RepairResource repairResource)
    {
        when(mockLockFactory.sufficientNodesForLocking(eq(repairResource.getDataCenter()), eq(repairResource.getResourceName(LOCKS_PER_RESOURCE)))).thenReturn(false);
    }
}
