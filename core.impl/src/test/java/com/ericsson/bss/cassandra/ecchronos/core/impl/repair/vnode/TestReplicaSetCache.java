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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicaSetCache
{
    @Mock
    private DriverNode mockNode1;
    @Mock
    private DriverNode mockNode2;
    @Mock
    private DriverNode mockNode3;

    private ReplicaSetCache cache;

    @Before
    public void setup()
    {
        cache = new ReplicaSetCache();
    }

    @Test
    public void testInternReturnsSameInstance()
    {
        ImmutableSet<DriverNode> set1 = ImmutableSet.of(mockNode1, mockNode2);
        ImmutableSet<DriverNode> set2 = ImmutableSet.of(mockNode1, mockNode2);

        ImmutableSet<DriverNode> interned1 = cache.intern(set1);
        ImmutableSet<DriverNode> interned2 = cache.intern(set2);

        assertThat(interned1).isSameAs(interned2);
    }

    @Test
    public void testDifferentSetsReturnDifferentInstances()
    {
        ImmutableSet<DriverNode> set1 = ImmutableSet.of(mockNode1, mockNode2);
        ImmutableSet<DriverNode> set2 = ImmutableSet.of(mockNode1, mockNode3);

        ImmutableSet<DriverNode> interned1 = cache.intern(set1);
        ImmutableSet<DriverNode> interned2 = cache.intern(set2);

        assertThat(interned1).isNotSameAs(interned2);
    }

    @Test
    public void testCacheSizeMatchesUniqueEntries()
    {
        ImmutableSet<DriverNode> set1 = ImmutableSet.of(mockNode1, mockNode2);
        ImmutableSet<DriverNode> set2 = ImmutableSet.of(mockNode2, mockNode3);

        cache.intern(set1);
        cache.intern(set1);
        cache.intern(set2);
        cache.intern(set2);

        assertThat(cache.size()).isEqualTo(2);
    }

    @Test
    public void testClearRemovesAllEntries()
    {
        cache.intern(ImmutableSet.of(mockNode1));
        cache.intern(ImmutableSet.of(mockNode2));
        assertThat(cache.size()).isEqualTo(2);

        cache.clear();

        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    public void testClearAllowsNewInstancesAfterwards()
    {
        ImmutableSet<DriverNode> set = ImmutableSet.of(mockNode1, mockNode2);
        ImmutableSet<DriverNode> first = cache.intern(set);

        cache.clear();

        ImmutableSet<DriverNode> second = cache.intern(ImmutableSet.of(mockNode1, mockNode2));
        assertThat(second).isEqualTo(first);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    public void testThreadSafety() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<ImmutableSet<DriverNode>>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            futures.add(executor.submit(() ->
            {
                latch.await();
                return cache.intern(ImmutableSet.of(mockNode1, mockNode2, mockNode3));
            }));
        }

        latch.countDown();

        ImmutableSet<DriverNode> expected = null;
        for (Future<ImmutableSet<DriverNode>> future : futures)
        {
            ImmutableSet<DriverNode> result = future.get();
            if (expected == null)
            {
                expected = result;
            }
            assertThat(result).isSameAs(expected);
        }

        assertThat(cache.size()).isEqualTo(1);
        executor.shutdown();
    }
}
