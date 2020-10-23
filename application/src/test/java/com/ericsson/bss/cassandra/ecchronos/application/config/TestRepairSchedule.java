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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.UnitConverter;
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

        assertThat(schedule.getRepairConfiguration("nonexisting", "keyspace")).isEmpty();
    }

    @Test
    public void testSettings() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("test_schedule.yml").getFile());

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

        RepairSchedule schedule = objectMapper.readValue(file, RepairSchedule.class);

        RepairConfiguration ks1tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .withRepairWarningTime(9, TimeUnit.DAYS)
                .withRepairErrorTime(11, TimeUnit.DAYS)
                .withRepairUnwindRatio(1.0d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("15m"))
                .build();

        RepairConfiguration ks1tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(5, TimeUnit.DAYS)
                .build();

        RepairConfiguration ks2tb1 = RepairConfiguration.newBuilder()
                .withRepairInterval(1, TimeUnit.DAYS)
                .withRepairWarningTime(5, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
                .withRepairUnwindRatio(0.5d)
                .withTargetRepairSizeInBytes(UnitConverter.toBytes("100m"))
                .build();

        assertThat(schedule.getRepairConfiguration("ks1", "tb1")).contains(ks1tb1);
        assertThat(schedule.getRepairConfiguration("ks1", "tb2")).contains(ks1tb2);
        assertThat(schedule.getRepairConfiguration("ks2", "tb1")).contains(ks2tb1);
    }

    @Test
    public void testRegex() throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File file = new File(classLoader.getResource("regex_schedule.yml").getFile());

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
                .build();
        RepairConfiguration ks2Tb2 = RepairConfiguration.newBuilder()
                .withRepairInterval(2, TimeUnit.DAYS)
                .build();

        assertThat(schedule.getRepairConfiguration("any", "nonexisting")).isEmpty();

        assertThat(schedule.getRepairConfiguration("any", "table_abc")).contains(allKeyspacesPattern);
        assertThat(schedule.getRepairConfiguration("ks2", "table_abc")).contains(allKeyspacesPattern);

        assertThat(schedule.getRepairConfiguration("any", "tb2")).contains(allKeyspacesTb2);

        assertThat(schedule.getRepairConfiguration("ks2", "tb1")).contains(ks2Tb1);
        assertThat(schedule.getRepairConfiguration("ks2", "tb2")).contains(ks2Tb2);

        assertThat(schedule.getRepairConfiguration("ks2", "tb23")).isEmpty();
    }
}
