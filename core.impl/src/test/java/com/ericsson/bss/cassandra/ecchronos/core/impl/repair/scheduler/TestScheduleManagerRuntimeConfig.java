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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.CASLockFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestScheduleManagerRuntimeConfig
{
    @Mock
    private CASLockFactory myLockFactory;

    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;

    @Mock
    private Node myNode;

    private final UUID myNodeID = UUID.randomUUID();
    private ScheduleManagerImpl myScheduler;

    @Before
    public void setup()
    {
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(myNodeID, myNode));
        myScheduler = ScheduleManagerImpl.builder()
                .withNodeIDList(Collections.singletonList(myNodeID))
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withLockFactory(myLockFactory)
                .withSessionWindow(5, TimeUnit.MINUTES)
                .withCooldown(10, TimeUnit.SECONDS)
                .build();
    }

    @After
    public void teardown()
    {
        myScheduler.close();
    }

    @Test
    public void testGetSessionWindowReturnsConfiguredValue()
    {
        assertThat(myScheduler.getSessionWindowInMs()).isEqualTo(TimeUnit.MINUTES.toMillis(5));
    }

    @Test
    public void testGetCooldownReturnsConfiguredValue()
    {
        assertThat(myScheduler.getCooldownInMs()).isEqualTo(TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void testSetSessionWindowUpdatesValue()
    {
        myScheduler.setSessionWindowInMs(600000L);
        assertThat(myScheduler.getSessionWindowInMs()).isEqualTo(600000L);
    }

    @Test
    public void testSetCooldownUpdatesValue()
    {
        myScheduler.setCooldownInMs(30000L);
        assertThat(myScheduler.getCooldownInMs()).isEqualTo(30000L);
    }

    @Test
    public void testSetCooldownToZeroIsAllowed()
    {
        myScheduler.setCooldownInMs(0L);
        assertThat(myScheduler.getCooldownInMs()).isEqualTo(0L);
    }

    @Test
    public void testSetSessionWindowToZeroThrows()
    {
        assertThatThrownBy(() -> myScheduler.setSessionWindowInMs(0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSetSessionWindowToNegativeThrows()
    {
        assertThatThrownBy(() -> myScheduler.setSessionWindowInMs(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSetCooldownToNegativeThrows()
    {
        assertThatThrownBy(() -> myScheduler.setCooldownInMs(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSetLocksPerResourceUpdatesValue()
    {
        int original = myScheduler.getLocksPerResource();
        try
        {
            myScheduler.setLocksPerResource(5);
            assertThat(myScheduler.getLocksPerResource()).isEqualTo(5);
        }
        finally
        {
            myScheduler.setLocksPerResource(original);
        }
    }

    @Test
    public void testSetLocksPerResourceToZeroThrows()
    {
        assertThatThrownBy(() -> myScheduler.setLocksPerResource(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
