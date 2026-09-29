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
package com.ericsson.bss.cassandra.ecchronos.core.impl.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.when;

import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestCassandraMetrics
{
    private static final Duration REFRESH_AFTER = Duration.ofMillis(200);
    private static final Duration EXPIRE_AFTER = Duration.ofMinutes(60);
    private static final Duration FETCH_TIMEOUT = Duration.ofMillis(500);

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private DistributedJmxProxy myJmxProxy;
    @Mock
    private TableReference myTableReference;

    private final UUID myNodeId = UUID.randomUUID();
    private CassandraMetrics myCassandraMetrics;

    @Before
    public void setup() throws Exception
    {
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
    }

    @After
    public void cleanup()
    {
        if (myCassandraMetrics != null)
        {
            myCassandraMetrics.close();
        }
    }

    private CassandraMetrics newMetrics()
    {
        return new CassandraMetrics(myJmxProxyFactory, REFRESH_AFTER, EXPIRE_AFTER, FETCH_TIMEOUT);
    }

    @Test
    public void testReadReturnsFetchedValues()
    {
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(1234L);
        when(myJmxProxy.getPercentRepaired(myNodeId, myTableReference)).thenReturn(50.0d);

        myCassandraMetrics = newMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(1234L);
        assertThat(myCassandraMetrics.getPercentRepaired(myNodeId, myTableReference)).isEqualTo(50.0d);
    }

    @Test(timeout = 10_000)
    public void testColdMissThatStallsFallsBackAfterTimeout() throws Exception
    {
        // The JMX fetch blocks longer than the fetch timeout; the read must not hang, it must fall back to 0.
        CountDownLatch release = new CountDownLatch(1);
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenAnswer(invocation ->
        {
            release.await(5, TimeUnit.SECONDS);
            return 1234L;
        });

        myCassandraMetrics = newMetrics();

        long start = System.currentTimeMillis();
        long maxRepairedAt = myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(maxRepairedAt).isEqualTo(0L);
        assertThat(elapsed).isLessThan(TimeUnit.SECONDS.toMillis(5));
        release.countDown();
    }

    @Test(timeout = 10_000)
    public void testRefreshRunsOffCallingThread() throws Exception
    {
        // First load is fast; a subsequent read after the refresh interval returns the cached value immediately
        // even while the background refresh is blocked, proving refreshes do not run on the calling thread.
        AtomicInteger call = new AtomicInteger();
        CountDownLatch secondFetchStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenAnswer(invocation ->
        {
            if (call.getAndIncrement() == 0)
            {
                return 100L;
            }
            secondFetchStarted.countDown();
            release.await(5, TimeUnit.SECONDS);
            return 200L;
        });

        myCassandraMetrics = newMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(100L);

        // Wait past the refresh interval so the next read triggers a background refresh.
        Thread.sleep(REFRESH_AFTER.toMillis() + 100);

        long start = System.currentTimeMillis();
        long value = myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference);
        long elapsed = System.currentTimeMillis() - start;

        // The read returned the previous (cached) value promptly, not blocked on the stalled refresh.
        assertThat(value).isEqualTo(100L);
        assertThat(elapsed).isLessThan(TimeUnit.SECONDS.toMillis(2));
        await().atMost(5, TimeUnit.SECONDS).until(() -> secondFetchStarted.getCount() == 0);
        release.countDown();
    }

    @Test(timeout = 10_000)
    public void testForceRefreshUpdatesValueSynchronously()
    {
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L, 200L);
        when(myJmxProxy.getPercentRepaired(myNodeId, myTableReference)).thenReturn(10.0d, 20.0d);

        myCassandraMetrics = newMetrics();

        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(100L);

        myCassandraMetrics.forceRefresh(myNodeId, myTableReference);

        // The forced refresh is synchronous, so the second value is visible immediately.
        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(200L);
    }

    @Test(timeout = 10_000)
    public void testForceRefreshKeepsPreviousValueOnFailure() throws Exception
    {
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenReturn(100L);
        when(myJmxProxy.getPercentRepaired(myNodeId, myTableReference)).thenReturn(10.0d);

        myCassandraMetrics = newMetrics();
        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(100L);

        // Next fetch stalls beyond the timeout; forceRefresh must not overwrite the last-known value.
        CountDownLatch release = new CountDownLatch(1);
        when(myJmxProxy.getMaxRepairedAt(myNodeId, myTableReference)).thenAnswer(invocation ->
        {
            release.await(5, TimeUnit.SECONDS);
            return 999L;
        });

        myCassandraMetrics.forceRefresh(myNodeId, myTableReference);

        assertThat(myCassandraMetrics.getMaxRepairedAt(myNodeId, myTableReference)).isEqualTo(100L);
        release.countDown();
    }
}
