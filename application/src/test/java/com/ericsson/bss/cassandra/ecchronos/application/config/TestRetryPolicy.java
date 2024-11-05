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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.Mock;
import org.mockito.MockMakers;

public class TestRetryPolicy {

    @Mock(mockMaker = MockMakers.SUBCLASS)
    RetryPolicy retryPolicy = Mockito.mock(RetryPolicy.class, Mockito.withSettings().mockMaker(MockMakers.SUBCLASS));

    @Before
    public void setup()
    {
        retryPolicy.setUnit("seconds");
        retryPolicy.setDelay(5);
        retryPolicy.setMaxAttempts(5);
        retryPolicy.setMaxDelay(30);
    }

    @Test
    public void testDefaultCurrentDelay() {
        long currentDelay = retryPolicy.currentDelay(1);

        assertEquals(TimeUnit.SECONDS.toMillis(10), currentDelay);
    }

    @Test
    public void testMaxDelay() {
        long currentDelay = retryPolicy.currentDelay(5);

        assertEquals(TimeUnit.SECONDS.toMillis(30), currentDelay);
    }

    @Test
    public void testMaxDelayDisabled() {

        retryPolicy.setMaxDelay(0);

        long currentDelay = retryPolicy.currentDelay(5);

        assertEquals(TimeUnit.SECONDS.toMillis(160), currentDelay);
    }

    @Test
    public void testDefaultConfigurationInMinutes() {
        retryPolicy.setUnit("minutes");
        retryPolicy.setDelay(5);
        retryPolicy.setMaxAttempts(5);
        retryPolicy.setMaxDelay(30);

        long currentDelay = retryPolicy.currentDelay(1);

        assertEquals(TimeUnit.MINUTES.toMillis(10), currentDelay);

        currentDelay = retryPolicy.currentDelay(5);

        assertEquals(TimeUnit.MINUTES.toMillis(30), currentDelay);

        retryPolicy.setMaxDelay(0);

        currentDelay = retryPolicy.currentDelay(5);

        assertEquals(TimeUnit.MINUTES.toMillis(160), currentDelay);
    }
}
