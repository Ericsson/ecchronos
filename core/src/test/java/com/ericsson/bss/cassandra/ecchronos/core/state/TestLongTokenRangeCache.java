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
package com.ericsson.bss.cassandra.ecchronos.core.state;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestLongTokenRangeCache
{
    @Before
    public void setup()
    {
        LongTokenRange.clearCache();
    }

    @After
    public void teardown()
    {
        LongTokenRange.clearCache();
    }

    @Test
    public void testCacheReturnsSameInstance()
    {
        LongTokenRange range1 = LongTokenRange.of(1, 100);
        LongTokenRange range2 = LongTokenRange.of(1, 100);

        assertThat(range1).isSameAs(range2);
    }

    @Test
    public void testCacheDifferentRangesReturnDifferentInstances()
    {
        LongTokenRange range1 = LongTokenRange.of(1, 100);
        LongTokenRange range2 = LongTokenRange.of(2, 200);

        assertThat(range1).isNotSameAs(range2);
    }

    @Test
    public void testCacheThreadSafety() throws Exception
    {
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<LongTokenRange>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            futures.add(executor.submit(() ->
            {
                latch.await();
                return LongTokenRange.of(42, 84);
            }));
        }

        latch.countDown();

        LongTokenRange expected = null;
        for (Future<LongTokenRange> future : futures)
        {
            LongTokenRange result = future.get();
            if (expected == null)
            {
                expected = result;
            }
            assertThat(result).isSameAs(expected);
        }

        executor.shutdown();
    }

    @Test
    public void testCacheSizeMatchesUniqueRanges()
    {
        for (int i = 0; i < 1000; i++)
        {
            LongTokenRange.of(i % 50, (i % 50) + 100);
        }

        assertThat(LongTokenRange.cacheSize()).isEqualTo(50);
    }

    @Test
    public void testEqualsConsistencyBetweenCachedAndConstructor()
    {
        LongTokenRange cached = LongTokenRange.of(10, 20);
        LongTokenRange constructed = new LongTokenRange(10, 20);

        assertThat(cached).isEqualTo(constructed);
        assertThat(cached.hashCode()).isEqualTo(constructed.hashCode());
    }

    @Test
    public void testClearCache()
    {
        LongTokenRange.of(1, 2);
        LongTokenRange.of(3, 4);
        assertThat(LongTokenRange.cacheSize()).isEqualTo(2);

        LongTokenRange.clearCache();
        assertThat(LongTokenRange.cacheSize()).isEqualTo(0);
    }
}
