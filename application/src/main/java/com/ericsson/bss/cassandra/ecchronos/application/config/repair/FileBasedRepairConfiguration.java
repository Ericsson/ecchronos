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
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;

import com.google.common.annotations.VisibleForTesting;

/** File-based repair configuration provider that reads schedules from a YAML file. */
public class FileBasedRepairConfiguration extends AbstractRepairConfigurationProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedRepairConfiguration.class);
    private static final String CONFIGURATION_FILE = "schedule.yml";

    private final RepairSchedule repairSchedule;

    /**
     * Constructs a new FileBasedRepairConfiguration using the default configuration file.
     *
     * @param applicationContext the Spring application context
     * @throws ConfigurationException if the configuration file cannot be read or parsed
     */
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
        validateRepairTypeMode(applicationContext);
    }

    /**
     * Enforce that per-table repair types in {@code schedule.yml} are consistent with the global repair type
     * in {@code ecc.yml}: a {@code unified_*} global type may not be mixed with legacy per-table types, and a
     * legacy global type may not be mixed with {@code unified_*} per-table types. The two scheduling
     * subsystems are mutually exclusive at runtime, so a mixed configuration is rejected at startup.
     *
     * @param applicationContext the Spring application context providing the global {@link Config}.
     * @throws ConfigurationException if a conflicting per-table repair type is configured.
     */
    private void validateRepairTypeMode(final ApplicationContext applicationContext) throws ConfigurationException
    {
        RepairType globalType = applicationContext.getBean(Config.class).getRepairConfig().getRepairType();
        boolean globalUnified = globalType.isUnified();

        List<String> conflicts = new ArrayList<>();
        for (RepairSchedule.TableRepairType tableType : repairSchedule.getConfiguredRepairTypes())
        {
            if (tableType.getRepairType().isUnified() != globalUnified)
            {
                conflicts.add(String.format("%s.%s=%s",
                        tableType.getKeyspace(), tableType.getTable(), tableType.getRepairType()));
            }
        }

        if (!conflicts.isEmpty())
        {
            String message = String.format(
                    "Mixed repair-type configuration is not supported: global repair_type is '%s' but "
                            + "schedule.yml overrides a conflicting family for: %s. A unified_* global type "
                            + "requires all per-table overrides to be unified_*, and a legacy global type "
                            + "requires all per-table overrides to be legacy (vnode/parallel_vnode/incremental).",
                    globalType, String.join(", ", conflicts));
            LOG.error(message);
            throw new ConfigurationException(message);
        }
    }

    @Override
    public final Set<RepairConfiguration> forTable(final TableReference tableReference)
    {
        return repairSchedule.getRepairConfigurations(tableReference.getKeyspace(), tableReference.getTable());
    }
}
