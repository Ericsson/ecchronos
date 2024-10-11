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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.utils.converter.UnitConverter;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import java.io.File;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class TestRepairSchedule
{
    @Test
    public void testDefault() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("schedule.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        assertThat(schedule.getRepairConfigurations("nonexisting", "keyspace")).isEmpty();
    }

    @Test
    public void testSettings() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/test_schedule.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        RepairConfiguration ks1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .withRepairUnwindRatio(1.0d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("15m"))
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();

        RepairConfiguration ks1tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();

        RepairConfiguration ks2tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairUnwindRatio(0.5d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("100m"))
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();

        assertThat(schedule.getRepairConfigurations("ks1", "tb1")).containsExactly(ks1tb1);
        assertThat(schedule.getRepairConfigurations("ks1", "tb2")).containsExactly(ks1tb2);
        assertThat(schedule.getRepairConfigurations("ks2", "tb1")).containsExactly(ks2tb1);
        assertThat(schedule.getRepairConfigurations("ks2", "tb2")).containsExactly(RepairConfiguration.DISABLED);
    }

    @Test
    public void testRegex() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/regex_schedule.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        RepairConfiguration allKeyspacesPattern = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .build();

        RepairConfiguration allKeyspacesTb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .build();

        RepairConfiguration ks2Tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();
        RepairConfiguration ks2Tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(2, TimeUnit.DAYS)
                .build();

        assertThat(schedule.getRepairConfigurations("any", "nonexisting")).isEmpty();

        assertThat(schedule.getRepairConfigurations("any", "table_abc")).containsExactly(allKeyspacesPattern);
        assertThat(schedule.getRepairConfigurations("ks2", "table_abc")).containsExactly(allKeyspacesPattern);

        assertThat(schedule.getRepairConfigurations("any", "tb2")).containsExactly(allKeyspacesTb2);

        assertThat(schedule.getRepairConfigurations("ks2", "tb1")).containsExactly(ks2Tb1);
        assertThat(schedule.getRepairConfigurations("ks2", "tb2")).containsExactly(ks2Tb2);

        assertThat(schedule.getRepairConfigurations("ks2", "tb23")).isEmpty();
    }

    @Test
    public void testMultipleSchedulesForSameTable() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/multiple_schedules.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        RepairConfiguration vnodeKs1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(6, TimeUnit.DAYS)
                .withRepairType(RepairType.VNODE)
                .build();

        RepairConfiguration incrementalKs1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairType(RepairType.INCREMENTAL)
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();

        assertThat(schedule.getRepairConfigurations("ks1", "tb1")).containsExactlyInAnyOrder(vnodeKs1tb1, incrementalKs1tb1);
    }

    @Test
    public void testMultipleSchedulesForSameTableRegex() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/multiple_schedules_regex.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        RepairConfiguration vnodeRegexTable = RepairConfiguration.newBuilder()
                .withRepairInterval(6, TimeUnit.DAYS)
                .withRepairType(RepairType.VNODE)
                .build();

        RepairConfiguration incrementalRegexTable = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairType(RepairType.INCREMENTAL)
                .withInitialDelay(1, TimeUnit.HOURS)
                .build();

        assertThat(schedule.getRepairConfigurations("ks1", "tb1")).containsExactlyInAnyOrder(vnodeRegexTable, incrementalRegexTable);
        assertThat(schedule.getRepairConfigurations("ks1", "tb2")).containsExactlyInAnyOrder(vnodeRegexTable, incrementalRegexTable);
    }

    @Test
    public void testRepairIntervalLongerThanWarn()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/schedule_repair_interval_longer_than_warn.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(JsonMappingException.class).isThrownBy(() -> objectMapper.readValue(file, RepairSchedule.class));
    }

    @Test
    public void testWarnIntervalLongerThanError()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("repair/schedule_warn_interval_longer_than_error.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        assertThatExceptionOfType(JsonMappingException.class).isThrownBy(() -> objectMapper.readValue(file, RepairSchedule.class));
    }
}
