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

import java.util.Optional;

import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.application.config.RepairSchedule;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;

public class FileBasedRepairConfiguration extends AbstractRepairConfigurationProvider
{
    private static final String CONFIGURATION_FILE = "schedule.yml";

    private final RepairSchedule repairSchedule;

    public FileBasedRepairConfiguration(ApplicationContext applicationContext) throws ConfigurationException
    {
        this(applicationContext, ConfigurationHelper.DEFAULT_INSTANCE, CONFIGURATION_FILE);
    }

    @VisibleForTesting
    FileBasedRepairConfiguration(ApplicationContext applicationContext, ConfigurationHelper configurationHelper,
            String configurationFile) throws ConfigurationException
    {
        super(applicationContext);

        repairSchedule = configurationHelper.getConfiguration(configurationFile, RepairSchedule.class);
    }

    @Override
    public Optional<RepairConfiguration> forTable(TableReference tableReference)
    {
        return repairSchedule.getRepairConfiguration(tableReference.getKeyspace(), tableReference.getTable());
    }
}
