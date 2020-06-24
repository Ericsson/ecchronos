/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestSchedulerProperties
{
    private static final long DEFAULT_RUN_INTERVAL = 30;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;

    @Test
    public void testDefaultValues() throws ConfigurationException
    {
        Properties properties = new Properties();

        SchedulerProperties schedulerProperties = SchedulerProperties.from(properties);

        assertThat(schedulerProperties.getRunInterval()).isEqualTo(DEFAULT_RUN_INTERVAL);
        assertThat(schedulerProperties.getTimeUnit()).isEqualTo(DEFAULT_TIME_UNIT);
    }

    @Test
    public void testSetAll() throws ConfigurationException
    {
        long expectedInterval = 15;
        TimeUnit expectedTimeUnit = TimeUnit.DAYS;
        Properties properties = new Properties();
        properties.put("scheduler.run.interval.time.unit", "days");
        properties.put("scheduler.run.interval.time", "15");

        SchedulerProperties schedulerProperties = SchedulerProperties.from(properties);

        assertThat(schedulerProperties.getRunInterval()).isEqualTo(expectedInterval);
        assertThat(schedulerProperties.getTimeUnit()).isEqualTo(expectedTimeUnit);
    }

    @Test
    public void testSetOnlyRunInterval()
    {
        Properties properties = new Properties();
        properties.put("scheduler.run.interval.time.unit", "days");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> SchedulerProperties.from(properties));
    }

    @Test
    public void testSetOnlyRunIntervalTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("scheduler.run.interval.time", "11");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> SchedulerProperties.from(properties));
    }

    @Test
    public void testSetInvalidTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("scheduler.run.interval.time.unit", "nonexisting");
        properties.put("scheduler.run.interval.time", "1");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> SchedulerProperties.from(properties));
    }

    @Test
    public void testSetInvalidRunInterval()
    {
        Properties properties = new Properties();
        properties.put("scheduler.run.interval.time.unit", "days");
        properties.put("scheduler.run.interval.time", "notANumber");

        assertThatExceptionOfType(NumberFormatException.class)
                .isThrownBy(() -> SchedulerProperties.from(properties));
    }
}
