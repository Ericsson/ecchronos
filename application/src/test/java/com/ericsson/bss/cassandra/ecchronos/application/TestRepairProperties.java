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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairOptions;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestRepairProperties
{
    private static final RepairOptions.RepairParallelism DEFAULT_REPAIR_PARALLELISM = RepairOptions.RepairParallelism.PARALLEL;
    private static final long DEFAULT_REPAIR_INTERVAL_IN_MS = TimeUnit.DAYS.toMillis(7);
    private static final long DEFAULT_ALARM_WARN_IN_MS = TimeUnit.DAYS.toMillis(8);
    private static final long DEFAULT_ALARM_ERROR_IN_MS = TimeUnit.DAYS.toMillis(10);
    private static final RepairLockType DEFAULT_REPAIR_LOCK_TYPE = RepairLockType.VNODE;
    private static final double DEFAULT_REPAIR_UNWIND_RATIO = 0.0d;

    @Test
    public void testDefaultValues() throws ConfigurationException
    {
        Properties properties = new Properties();

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
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
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
    }

    @Test
    public void testSetParallelism() throws ConfigurationException
    {
        RepairOptions.RepairParallelism expectedParallelism = RepairOptions.RepairParallelism.PARALLEL;

        Properties properties = new Properties();
        properties.put("repair.parallelism", "parallel");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(expectedParallelism);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
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
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(expectedAlarmWarnInMs);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
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
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(expectedAlarmErrorInMs);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
    }

    @Test
    public void testSetRepairLockType() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.lock.type", "datacenter_and_vnode");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(RepairLockType.DATACENTER_AND_VNODE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(DEFAULT_REPAIR_UNWIND_RATIO);
    }

    @Test
    public void testSetRepairUnwindRatio() throws ConfigurationException
    {
        Properties properties = new Properties();
        properties.put("repair.unwind.ratio", "1.0");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(DEFAULT_REPAIR_INTERVAL_IN_MS);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(DEFAULT_REPAIR_PARALLELISM);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(DEFAULT_ALARM_WARN_IN_MS);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(DEFAULT_ALARM_ERROR_IN_MS);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(DEFAULT_REPAIR_LOCK_TYPE);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(1.0d);
    }

    @Test
    public void testSetAll() throws ConfigurationException
    {
        long expectedRepairIntervalInMs = TimeUnit.DAYS.toMillis(1);
        RepairOptions.RepairParallelism expectedParallelism = RepairOptions.RepairParallelism.PARALLEL;
        long expectedAlarmWarnInMs = TimeUnit.DAYS.toMillis(5);
        long expectedAlarmErrorInMs = TimeUnit.DAYS.toMillis(7);

        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "days");
        properties.put("repair.interval.time", "1");
        properties.put("repair.parallelism", "parallel");
        properties.put("repair.alarm.warn.time.unit", "days");
        properties.put("repair.alarm.warn.time", "5");
        properties.put("repair.alarm.error.time.unit", "days");
        properties.put("repair.alarm.error.time", "7");
        properties.put("repair.lock.type", "datacenter");
        properties.put("repair.unwind.ratio", "1.0");

        RepairProperties repairProperties = RepairProperties.from(properties);

        assertThat(repairProperties.getRepairIntervalInMs()).isEqualTo(expectedRepairIntervalInMs);
        assertThat(repairProperties.getRepairParallelism()).isEqualTo(expectedParallelism);
        assertThat(repairProperties.getRepairAlarmWarnInMs()).isEqualTo(expectedAlarmWarnInMs);
        assertThat(repairProperties.getRepairAlarmErrorInMs()).isEqualTo(expectedAlarmErrorInMs);
        assertThat(repairProperties.getRepairLockType()).isEqualTo(RepairLockType.DATACENTER);
        assertThat(repairProperties.getRepairUnwindRatio()).isEqualTo(1.0d);

    }

    @Test
    public void testSetInvalidParallelism()
    {
        Properties properties = new Properties();
        properties.put("repair.parallelism", "nonexisting");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetOnlyIntervalTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "days");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetOnlyAlarmWarnTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.warn.time.unit", "days");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetOnlyAlarmErrorTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.error.time.unit", "days");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetInvalidIntervalTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.interval.time.unit", "nonexisting");
        properties.put("repair.interval.time", "1");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetInvalidAlarmWarnTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.warn.time.unit", "nonexisting");
        properties.put("repair.alarm.warn.time", "1");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetInvalidAlarmErrorTimeUnit()
    {
        Properties properties = new Properties();
        properties.put("repair.alarm.error.time.unit", "nonexisting");
        properties.put("repair.alarm.warn.time", "1");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }

    @Test
    public void testSetInvalidRepairLockType()
    {
        Properties properties = new Properties();
        properties.put("repair.lock.type", "nonexisting");

        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> RepairProperties.from(properties));
    }
}
