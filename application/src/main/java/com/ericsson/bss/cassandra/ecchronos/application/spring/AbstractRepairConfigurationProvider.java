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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.util.HashSet;
import java.util.Set;

import org.springframework.context.ApplicationContext;

/**
 * Abstract base class for providing repair configurations for tables.
 * Subclasses supply table-specific configurations; when none exist the default
 * repair configuration from the application config is used.
 */
public abstract class AbstractRepairConfigurationProvider
{
    private final ApplicationContext applicationContext;

    /**
     * Returns the Spring application context.
     *
     * @return the application context.
     */
    public final ApplicationContext getApplicationContext()
    {
        return applicationContext;
    }

    private final RepairConfiguration defaultRepairConfiguration;

    /**
     * Constructs a new provider using the given application context.
     * The default repair configuration is derived from the application config bean.
     *
     * @param anApplicationContext the Spring application context.
     */
    protected AbstractRepairConfigurationProvider(final ApplicationContext anApplicationContext)
    {
        this.applicationContext = anApplicationContext;

        Config config = applicationContext.getBean(Config.class);
        this.defaultRepairConfiguration = config.getRepairConfig().asRepairConfiguration();
    }

    /**
     * Returns the set of repair configurations for the given table.
     * If no table-specific configurations are provided by the subclass,
     * the default repair configuration is returned.
     *
     * @param tableReference the table to get repair configurations for.
     * @return a non-empty set of repair configurations.
     */
    public final Set<RepairConfiguration> get(final TableReference tableReference)
    {
        Set<RepairConfiguration> repairConfigurations = new HashSet<>();
        repairConfigurations.addAll(forTable(tableReference));
        if (repairConfigurations.isEmpty())
        {
            repairConfigurations.add(defaultRepairConfiguration);
        }
        return repairConfigurations;
    }

    /**
     * Returns table-specific repair configurations. Implementations may return an
     * empty set to fall back to the default configuration.
     *
     * @param tableReference the table reference to look up configurations for.
     * @return a set of repair configurations for the table, possibly empty.
     */
    public abstract Set<RepairConfiguration> forTable(TableReference tableReference);
}


