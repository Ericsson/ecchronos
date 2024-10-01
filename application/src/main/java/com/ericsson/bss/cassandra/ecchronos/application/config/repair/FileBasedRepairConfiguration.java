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

import com.ericsson.bss.cassandra.ecchronos.application.spring.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import java.util.Set;

import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;

import com.google.common.annotations.VisibleForTesting;

public class FileBasedRepairConfiguration extends AbstractRepairConfigurationProvider
{
    private static final String CONFIGURATION_FILE = "schedule.yml";

    private final RepairSchedule repairSchedule;

    public FileBasedRepairConfiguration(final ApplicationContext applicationContext) throws ConfigurationException
    {
        this(applicationContext, ConfigurationHelper.DEFAULT_INSTANCE, CONFIGURATION_FILE);
    }

    @VisibleForTesting
    FileBasedRepairConfiguration(final ApplicationContext applicationContext,
            final ConfigurationHelper configurationHelper,
            final String configurationFile) throws ConfigurationException
    {
        super(applicationContext);

        repairSchedule = configurationHelper.getConfiguration(configurationFile, RepairSchedule.class);
    }

    @Override
    public final Set<RepairConfiguration> forTable(final TableReference tableReference)
    {
        return repairSchedule.getRepairConfigurations(tableReference.getKeyspace(), tableReference.getTable());
    }
}
