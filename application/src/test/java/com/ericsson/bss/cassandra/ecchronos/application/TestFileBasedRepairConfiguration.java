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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

@RunWith(MockitoJUnitRunner.class)
public class TestFileBasedRepairConfiguration
{
    @Mock
    private ApplicationContext mockApplicationContext;

    @Mock
    private Config config;

    @Before
    public void setup()
    {
        when(mockApplicationContext.getBean(eq(Config.class))).thenReturn(config);
        when(config.getRepairConfig()).thenReturn(new GlobalRepairConfig());
    }

    @Test
    public void testNoSchedule() throws Exception
    {
        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("schedule.yml");
        assertThat(repairConfigProvider.get(tableReference("any", "table"))).containsExactly(RepairConfiguration.DEFAULT);
    }


    @Test
    public void testAllSchedules() throws Exception
    {
        AbstractRepairConfigurationProvider repairConfigProvider = withSchedule("repair/regex_schedule.yml");

        RepairConfiguration allKeyspacesPattern = RepairConfiguration.newBuilder()
                .withRepairInterval(8, TimeUnit.DAYS)
                .withRepairWarningTime(9, TimeUnit.DAYS)
                .withRepairErrorTime(10, TimeUnit.DAYS)
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

        assertConfig(repairConfigProvider, "any", "nonexisting", RepairConfiguration.DEFAULT);

        assertConfig(repairConfigProvider, "any", "table_abc", allKeyspacesPattern);
        assertConfig(repairConfigProvider, "ks2", "table_abc", allKeyspacesPattern);

        assertConfig(repairConfigProvider, "any", "tb2", allKeyspacesTb2);

        assertConfig(repairConfigProvider, "ks2", "tb1", ks2Tb1);
        assertConfig(repairConfigProvider, "ks2", "tb2", ks2Tb2);

        assertConfig(repairConfigProvider, "ks2", "tb23", RepairConfiguration.DEFAULT);

        assertConfig(repairConfigProvider, "any", "table", RepairConfiguration.DEFAULT);
    }

    @Test (expected = ConfigurationException.class)
    public void testNullScheduleConfig() throws Exception
    {
        withSchedule("repair/null_schedule.yml");
    }

    private void assertConfig(AbstractRepairConfigurationProvider repairConfigProvider, String keyspace, String table,
            RepairConfiguration repairConfiguration)
    {
        assertThat(repairConfigProvider.get(tableReference(keyspace, table))).containsExactly(repairConfiguration);
    }

    private AbstractRepairConfigurationProvider withSchedule(String schedule) throws Exception
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        File configFile = new File(classLoader.getResource(schedule).toURI());
        ConfigurationHelper configurationHelper = new ConfigurationHelper(configFile.getParent());

        return new FileBasedRepairConfiguration(mockApplicationContext, configurationHelper, schedule);
    }

    private TableReference tableReference(String keyspace, String table)
    {
        TableReference tableReference = mock(TableReference.class);
        when(tableReference.getKeyspace()).thenReturn(keyspace);
        when(tableReference.getTable()).thenReturn(table);
        return tableReference;
    }
}
