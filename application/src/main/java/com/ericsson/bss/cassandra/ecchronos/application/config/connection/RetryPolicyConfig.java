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

/**
 * Configuration for the retry policy used when reconnecting to unavailable nodes.
 * Contains settings for maximum retry attempts, delay between retries, and scheduled retry intervals.
 */
public final class RetryPolicyConfig
{
    /** Default constructor for Jackson deserialization. */
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

    /**
     * Returns the maximum number of retry attempts.
     *
     * @return the maximum number of retry attempts
     */
    @JsonProperty ("maxAttempts")
    public Integer getMaxAttempts()
    {
        return myMaxAttempts;
    }

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param maxAttempts the maximum number of retry attempts, or null to keep the default
     */
    @JsonProperty("maxAttempts")
    public void setMaxAttempts(final Integer maxAttempts)
    {
        if (maxAttempts != null)
        {
            this.myMaxAttempts = maxAttempts;
        }
    }

    /**
     * Sets the retry delay configuration.
     *
     * @param retryDelay the retry delay configuration
     */
    @JsonProperty("delay")
    public void setRetryDelay(final RetryDelay retryDelay)
    {
        myRetryDelay = retryDelay;
    }

    /**
     * Returns the retry delay configuration.
     *
     * @return the retry delay configuration
     */
    @JsonProperty("delay")
    public RetryDelay getRetryDelay()
    {
        return myRetryDelay;
    }

    /**
     * Returns the retry schedule configuration.
     *
     * @return the retry schedule configuration
     */
    @JsonProperty("retrySchedule")
    public RetrySchedule getRetrySchedule()
    {
        return myRetrySchedule;
    }

    /**
     * Sets the retry schedule configuration.
     *
     * @param retrySchedule the retry schedule configuration
     */
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
     * Configuration for retry delay parameters including start delay, maximum delay, and time unit.
     */
    public static final class RetryDelay
    {
        /** Default constructor for Jackson deserialization. */
        public RetryDelay()
        {

        }

        @JsonProperty("start")
        private long myDelay = DEFAULT_DELAY_IN_MS;

        @JsonProperty("max")
        private long myMaxDelay = DEFAULT_MAX_DELAY_IN_MS;

        @JsonProperty("unit")
        private TimeUnit myTimeUnit = DEFAULT_TIME_UNIT_IN_SECONDS;

        /**
         * Returns the start delay in milliseconds.
         *
         * @return the start delay in milliseconds
         */
        @JsonProperty("start")
        public long getStartDelay()
        {
            return myDelay;
        }

        /**
         * Sets the start delay. The value is converted to milliseconds using the configured time unit.
         *
         * @param delay the start delay in the configured time unit, or null to keep the default
         * @throws IllegalArgumentException if the converted delay is greater than the maximum delay
         */
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

        /**
         * Returns the maximum delay in milliseconds.
         *
         * @return the maximum delay in milliseconds
         */
        @JsonProperty("max")
        public long getMaxDelay()
        {
            return myMaxDelay;
        }

        /**
         * Sets the maximum delay. The value is converted to milliseconds using the configured time unit.
         *
         * @param maxDelay the maximum delay in the configured time unit, or null to keep the default
         * @throws IllegalArgumentException if the converted max delay is less than the start delay
         */
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

        /**
         * Returns the time unit used for delay values.
         *
         * @return the time unit
         */
        @JsonProperty("unit")
        public TimeUnit getUnit()
        {
            return myTimeUnit;
        }

        /**
         * Sets the time unit used for delay values.
         *
         * @param unit the time unit name (e.g., "SECONDS", "MILLISECONDS"), or null/blank to keep the default
         */
        @JsonProperty("unit")
        public void setTimeUnit(final String unit)
        {
            if (unit != null && !unit.isBlank())
            {
                myTimeUnit = TimeUnit.valueOf(unit.toUpperCase(Locale.US));
            }
        }
    }

    /**
     * Configuration for the retry schedule including initial delay and fixed delay between retry cycles.
     */
    public static final class RetrySchedule
    {
        /** Default constructor for Jackson deserialization. */
        public RetrySchedule()
        {

        }

        @JsonProperty("initialDelay")
        private long myInitialDelay = DEFAULT_INITIAL_DELAY_IN_MS;

        @JsonProperty("fixedDelay")
        private long myFixedDelay = DEFAULT_FIXED_DELAY_IN_MS;

        @JsonProperty("unit")
        private TimeUnit myTimeUnit = DEFAULT_TIME_UNIT_IN_SECONDS;

        /**
         * Returns the initial delay in milliseconds before the first retry cycle.
         *
         * @return the initial delay in milliseconds
         */
        @JsonProperty("initialDelay")
        public long getInitialDelay()
        {
            return myInitialDelay;
        }

        /**
         * Sets the initial delay before the first retry cycle. The value is converted to milliseconds
         * using the configured time unit.
         *
         * @param initialDelay the initial delay in the configured time unit, or null to keep the default
         */
        @JsonProperty("initialDelay")
        public void setInitialDelay(final Long initialDelay)
        {
            if (initialDelay != null)
            {
                this.myInitialDelay = convertToMillis(initialDelay, myTimeUnit);
            }
        }

        /**
         * Returns the fixed delay in milliseconds between retry cycles.
         *
         * @return the fixed delay in milliseconds
         */
        @JsonProperty("fixedDelay")
        public long getFixedDelay()
        {
            return myFixedDelay;
        }

        /**
         * Sets the fixed delay between retry cycles. The value is converted to milliseconds
         * using the configured time unit.
         *
         * @param fixedDelay the fixed delay in the configured time unit, or null to keep the default
         */
        @JsonProperty("fixedDelay")
        public void setFixedDelay(final Long fixedDelay)
        {
            if (fixedDelay != null)
            {
                this.myFixedDelay = convertToMillis(fixedDelay, myTimeUnit);
            }
        }

        /**
         * Returns the time unit used for schedule delay values.
         *
         * @return the time unit
         */
        @JsonProperty("unit")
        public TimeUnit getUnit()
        {
            return myTimeUnit;
        }

        /**
         * Sets the time unit used for schedule delay values.
         *
         * @param unit the time unit name (e.g., "SECONDS", "MILLISECONDS"), or null/blank to keep the default
         */
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
