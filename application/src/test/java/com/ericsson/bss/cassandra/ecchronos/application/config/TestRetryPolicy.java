package com.ericsson.bss.cassandra.ecchronos.application.config;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestRetryPolicy {

    RetryPolicy retryPolicy = Mockito.mock(RetryPolicy.class);

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
