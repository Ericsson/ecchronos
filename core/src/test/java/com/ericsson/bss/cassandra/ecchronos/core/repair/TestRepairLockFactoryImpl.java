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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TestRepairLockFactoryImpl
{
    @Mock
    private LockFactory mockLockFactory;

    @Mock
    private LockFactory.DistributedLock mockLock;

    @Test
    public void testNothingToLockThrowsException()
    {
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");

        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(), metadata, 1));
    }

    @Test
    public void testSingleLock() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-1"));
        doReturn(mockLock).when(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-1"), eq(priority), eq(metadata));

        try (LockFactory.DistributedLock lock = repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResource), metadata, priority))
        {
        }

        verify(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-1"), eq(priority), eq(metadata));
    }

    @Test
    public void testSingleLockNotSufficientNodes() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(false).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-1"));
        doReturn(mockLock).when(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-1"), eq(priority), eq(metadata));

        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResource), metadata, priority));
    }

    @Test
    public void testSingleLockFailing() throws LockException
    {
        RepairResource repairResource = new RepairResource("DC1", "my-resource");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-1"));
        doThrow(LockException.class).when(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-1"), eq(priority), eq(metadata));

        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResource), metadata, priority));
    }

    @Test
    public void testMultipleLocks() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-dc1-1"));
        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC2"), eq("RepairResource-my-resource-dc2-1"));
        doReturn(mockLock).when(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-dc1-1"), eq(priority), eq(metadata));
        doReturn(mockLock).when(mockLockFactory).tryLock(eq("DC2"), eq("RepairResource-my-resource-dc2-1"), eq(priority), eq(metadata));

        try (LockFactory.DistributedLock lock = repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResourceDc1, repairResourceDc2), metadata, priority))
        {

        }

        verify(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-dc1-1"), eq(priority), eq(metadata));
        verify(mockLockFactory).tryLock(eq("DC2"), eq("RepairResource-my-resource-dc2-1"), eq(priority), eq(metadata));
    }

    @Test
    public void testMultipleLocksNotSufficientNodes()
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-dc1-1"));
        doReturn(false).when(mockLockFactory).sufficientNodesForLocking(eq("DC2"), eq("RepairResource-my-resource-dc2-1"));

        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResourceDc1, repairResourceDc2), metadata, priority));
    }

    @Test
    public void testMultipleLocksOneFailing() throws LockException
    {
        RepairResource repairResourceDc1 = new RepairResource("DC1", "my-resource-dc1");
        RepairResource repairResourceDc2 = new RepairResource("DC2", "my-resource-dc2");
        RepairLockFactoryImpl repairLockFactory = new RepairLockFactoryImpl();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("metadataKey", "metadataValue");
        int priority = 1;

        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC1"), eq("RepairResource-my-resource-dc1-1"));
        doReturn(true).when(mockLockFactory).sufficientNodesForLocking(eq("DC2"), eq("RepairResource-my-resource-dc2-1"));
        doReturn(mockLock).when(mockLockFactory).tryLock(eq("DC1"), eq("RepairResource-my-resource-dc1-1"), eq(priority), eq(metadata));
        doThrow(LockException.class).when(mockLockFactory).tryLock(eq("DC2"), eq("RepairResource-my-resource-dc2-1"), eq(priority), eq(metadata));

        assertThatExceptionOfType(LockException.class)
                .isThrownBy(() -> repairLockFactory.getLock(mockLockFactory, Sets.newHashSet(repairResourceDc1, repairResourceDc2), metadata, priority));
    }
}
