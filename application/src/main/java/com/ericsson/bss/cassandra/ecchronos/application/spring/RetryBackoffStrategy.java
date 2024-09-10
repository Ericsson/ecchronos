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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.RetryPolicyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RetryBackoffStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(RetryBackoffStrategy.class);
    private static final int ONE_SECOND_IN_MS = 1000;
    private static final int RETRY_DELAY_MULTIPLIER = 2;
    private final RetryPolicyConfig myRetryPolicyConfig;
    private final RetryPolicyConfig.RetrySchedule myRetrySchedule;
    private final RetryPolicyConfig.RetryDelay myRetryDelay;

    public RetryBackoffStrategy(final RetryPolicyConfig retryPolicyConfig)
    {
        myRetryPolicyConfig = retryPolicyConfig;
        myRetrySchedule = retryPolicyConfig.getRetrySchedule();
        myRetryDelay = retryPolicyConfig.getRetryDelay();
    }

    public long getInitialDelay()
    {
        return myRetrySchedule.getInitialDelay();
    }

    public long getFixedDelay()
    {
        return myRetrySchedule.getFixedDelay();
    }

    public int getMaxAttempts()
    {
        return myRetryPolicyConfig.getMaxAttempts();
    }

    public long calculateDelay(final int attempt)
    {
        long baseDelay = myRetryDelay.getStartDelay();
        long calculatedDelay = baseDelay * ((long) attempt * RETRY_DELAY_MULTIPLIER);
        LOG.debug("Calculated delay for attempt {}: {} ms", attempt, calculatedDelay);
        return Math.min(calculatedDelay, myRetryDelay.getMaxDelay() * ONE_SECOND_IN_MS);
    }

    public void sleepBeforeNextRetry(final long delayMillis)
    {
        try
        {
            Thread.sleep(delayMillis);
        }
        catch (InterruptedException e)
        {
            LOG.error("Interrupted during sleep between retry attempts", e);
            Thread.currentThread().interrupt();
        }
    }
}
