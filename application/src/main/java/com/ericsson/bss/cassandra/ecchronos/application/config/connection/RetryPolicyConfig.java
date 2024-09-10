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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class RetryPolicyConfig
{
    public RetryPolicyConfig()
    {
    }

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final long DEFAULT_DELAY_IN_MS = 5000;
    private static final long DEFAULT_MAX_DELAY_IN_MS = 30000;
    private static final long DEFAULT_INITIAL_DELAY_IN_MS = 86400000;
    private static final long DEFAULT_FIXED_DELAY_IN_MS = 86400000;
    private static final TimeUnit DEFAULT_TIME_UNIT_IN_SECONDS = TimeUnit.SECONDS;
    private RetryPolicyConfig.RetryDelay myRetryDelay = new RetryPolicyConfig.RetryDelay();
    private RetryPolicyConfig.RetrySchedule myRetrySchedule = new RetryPolicyConfig.RetrySchedule();

    @JsonProperty("maxAttempts")
    private Integer myMaxAttempts = DEFAULT_MAX_ATTEMPTS;

    @JsonProperty ("maxAttempts")
    public Integer getMaxAttempts()
    {
        return myMaxAttempts;
    }

    @JsonProperty("maxAttempts")
    public void setMaxAttempts(final Integer maxAttempts)
    {
        if (maxAttempts != null)
        {
            this.myMaxAttempts = maxAttempts;
        }
    }

    @JsonProperty("delay")
    public void setRetryDelay(final RetryDelay retryDelay)
    {
        myRetryDelay = retryDelay;
    }

    @JsonProperty("delay")
    public RetryDelay getRetryDelay()
    {
        return myRetryDelay;
    }

    @JsonProperty("retrySchedule")
    public RetrySchedule getRetrySchedule()
    {
        return myRetrySchedule;
    }

    @JsonProperty("retrySchedule")
    public void setRetrySchedule(final RetrySchedule retrySchedule)
    {
        myRetrySchedule = retrySchedule;
    }

    private static long convertToMillis(final Long value, final TimeUnit unit)
    {
        return unit.toMillis(value);
    }

    /**
     * Configuration for retry delay parameter.
     */
    public static final class RetryDelay
    {
        public RetryDelay()
        {

        }

        @JsonProperty("start")
        private long myDelay = DEFAULT_DELAY_IN_MS;

        @JsonProperty("max")
        private long myMaxDelay = DEFAULT_MAX_DELAY_IN_MS;

        @JsonProperty("unit")
        private TimeUnit myTimeUnit = DEFAULT_TIME_UNIT_IN_SECONDS;

        @JsonProperty("start")
        public long getStartDelay()
        {
            return myDelay;
        }

        @JsonProperty("start")
        public void setStartDelay(final Long delay)
        {
            if (delay != null)
            {
                long convertedDelay = convertToMillis(delay, myTimeUnit);
                if (convertedDelay > myMaxDelay)
                {
                    throw new IllegalArgumentException("Start delay cannot be greater than max delay.");
                }
                this.myDelay = convertToMillis(delay, myTimeUnit);
            }
        }

        @JsonProperty("max")
        public long getMaxDelay()
        {
            return myMaxDelay;
        }

        @JsonProperty("max")
        public void setMaxDelay(final Long maxDelay)
        {
            if (maxDelay != null)
            {
                long convertedMaxDelay = convertToMillis(maxDelay, myTimeUnit);
                if (convertedMaxDelay < myDelay)
                {
                    throw new IllegalArgumentException("Max delay cannot be less than start delay.");
                }
                this.myMaxDelay = convertToMillis(maxDelay, myTimeUnit);
            }
        }

        @JsonProperty("unit")
        public TimeUnit getUnit()
        {
            return myTimeUnit;
        }

        @JsonProperty("unit")
        public void setTimeUnit(final String unit)
        {
            if (unit != null && !unit.isBlank())
            {
                myTimeUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
            }
        }
    }

    public static final class RetrySchedule
    {
        public RetrySchedule()
        {

        }

        @JsonProperty("initialDelay")
        private long myInitialDelay = DEFAULT_INITIAL_DELAY_IN_MS;

        @JsonProperty("fixedDelay")
        private long myFixedDelay = DEFAULT_FIXED_DELAY_IN_MS;

        @JsonProperty("unit")
        private TimeUnit myTimeUnit = DEFAULT_TIME_UNIT_IN_SECONDS;

        @JsonProperty("initialDelay")
        public long getInitialDelay()
        {
            return myInitialDelay;
        }

        @JsonProperty("initialDelay")
        public void setInitialDelay(final Long initialDelay)
        {
            if (initialDelay != null)
            {
                this.myInitialDelay = convertToMillis(initialDelay, myTimeUnit);
            }
        }

        @JsonProperty("fixedDelay")
        public long getFixedDelay()
        {
            return myFixedDelay;
        }

        @JsonProperty("fixedDelay")
        public void setFixedDelay(final Long fixedDelay)
        {
            if (fixedDelay != null)
            {
                this.myFixedDelay = convertToMillis(fixedDelay, myTimeUnit);
            }
        }

        @JsonProperty("unit")
        public TimeUnit getUnit()
        {
            return myTimeUnit;
        }

        @JsonProperty("unit")
        public void setTimeUnit(final String unit)
        {
            if (unit != null && !unit.isBlank())
            {
                myTimeUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
            }
        }
    }
}
