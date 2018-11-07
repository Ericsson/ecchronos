/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TestRepairProperties
{
    private static final RepairOptions.RepairType DEFAULT_REPAIR_TYPE = RepairOptions.RepairType.VNODE;
    private static final RepairOptions.RepairParallelism DEFAULT_REPAIR_PARALLELISM = RepairOptions.RepairParallelism.PARALLEL;
    private static final long DEFAULT_REPAIR_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(7);
    private static final long DEFAULT_ALARM_WARN_IN_MS = TimeUnit.DAYS.toMillis(8);
    private static final long DEFAULT_ALARM_ERROR_IN_MS = TimeUnit.DAYS.toMillis(10);

    @Test
    public void testDefaultValues() throws ConfigurationException
    {
        Properties properties = new Properties();

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
    }

    @Test
    public void testSetRepairInterval() throws ConfigurationException
    {
        long expectedRepairIntervalInMs = TimeUnit.DAYS.toMillis(1);

        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "days");
        properties.put("repair.interval.time", "1");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(expectedRepairIntervalInMs);
        assertThat(repairProperties.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
    }

    @Test
    public void testSetType() throws ConfigurationException
    {
        RepairOptions.RepairType expectedType = RepairOptions.RepairType.INCREMENTAL;

        Properties properties = new Properties();
        properties.put("repair.type", "incremental");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairType()).isEqualTo(expectedType);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
    }

    @Test
    public void testSetParallelism() throws ConfigurationException
    {
        RepairOptions.RepairParallelism expectedParallelism = RepairOptions.RepairParallelism.PARALLEL;

        Properties properties = new Properties();
        properties.put("repair.parallelism", "parallel");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(expectedParallelism);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
    }

    @Test
    public void testSetAlarmWarn() throws ConfigurationException
    {
        long expectedAlarmWarnInMs = TimeUnit.DAYS.toMillis(7);

        Properties properties = new Properties();
        properties.put("repair.alarm.warn.time.unit", "days");
        properties.put("repair.alarm.warn.time", "7");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(expectedAlarmWarnInMs);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
    }

    @Test
    public void testSetAlarmError() throws ConfigurationException
    {
        long expectedAlarmErrorInMs = TimeUnit.DAYS.toMillis(9);

        Properties properties = new Properties();
        properties.put("repair.alarm.error.time.unit", "days");
        properties.put("repair.alarm.error.time", "9");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairType()).isEqualTo(DEFAULT_REPAIR_TYPE);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(expectedAlarmErrorInMs);
    }

    @Test
    public void testSetAll() throws ConfigurationException
    {
        long expectedRepairIntervalInMs = TimeUnit.DAYS.toMillis(1);
        RepairOptions.RepairType expectedType = RepairOptions.RepairType.INCREMENTAL;
        RepairOptions.RepairParallelism expectedParallelism = RepairOptions.RepairParallelism.PARALLEL;
        long expectedAlarmWarnInMs = TimeUnit.DAYS.toMillis(5);
        long expectedAlarmErrorInMs = TimeUnit.DAYS.toMillis(7);

        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "days");
        properties.put("repair.interval.time", "1");
        properties.put("repair.type", "incremental");
        properties.put("repair.parallelism", "parallel");
        properties.put("repair.alarm.warn.time.unit", "days");
        properties.put("repair.alarm.warn.time", "5");
        properties.put("repair.alarm.error.time.unit", "days");
        properties.put("repair.alarm.error.time", "7");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(expectedRepairIntervalInMs);
        assertThat(repairProperties.getRepairType()).isEqualTo(expectedType);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(expectedParallelism);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(expectedAlarmWarnInMs);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(expectedAlarmErrorInMs);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetInvalidType() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.type", "nonexisting");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetInvalidParallelism() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.parallelism", "nonexisting");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetOnlyIntervalTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "days");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetOnlyAlarmWarnTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.warn.time.unit", "days");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetOnlyAlarmErrorTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.error.time.unit", "days");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetInvalidIntervalTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "nonexisting");
        properties.put("repair.interval.time", "1");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetInvalidAlarmWarnTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.warn.time.unit", "nonexisting");
        properties.put("repair.alarm.warn.time", "1");

        RepairProperties.from(properties);
    }

    @Test (expected = ConfigurationException.class)
    public void testSetInvalidAlarmErrorTimeUnit() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.error.time.unit", "nonexisting");
        properties.put("repair.alarm.warn.time", "1");

        RepairProperties.from(properties);
    }
}
