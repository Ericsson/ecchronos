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

import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for per-table repair schedules. */
public class RepairSchedule
{
    private Map<String, KeyspaceSchedule> myKeyspaceSchedules = new HashMap<>();

    /** Default constructor. */
    public RepairSchedule()
    {
    }

    /**
     * Sets the keyspace schedules.
     * @param keyspaceSchedules the keyspace schedules
     */
    @JsonProperty("keyspaces")
    public final void setKeyspaceSchedules(final List<KeyspaceSchedule> keyspaceSchedules)
    {
        if (keyspaceSchedules != null)
        {
            myKeyspaceSchedules = keyspaceSchedules.stream().collect(
                    Collectors.toMap(KeyspaceSchedule::getKeyspaceName, ks -> ks));
        }
    }

    /**
     * Returns the repair configurations.
     * @param keyspaceName the keyspace name
     * @param tableName the table name
     * @return the repair configurations
     */
    public final Set<RepairConfiguration> getRepairConfigurations(final String keyspaceName, final String tableName)
    {
        return findMatching(keyspaceName, myKeyspaceSchedules,
                keyspaceSchedule -> keyspaceSchedule.getRepairConfiguration(tableName));
    }

    /**
     * Enumerate every explicitly configured per-table repair type, labelled by keyspace and table, so the
     * repair-type mode can be validated against the global configuration.
     *
     * @return the list of configured table repair types.
     */
    public final List<TableRepairType> getConfiguredRepairTypes()
    {
        List<TableRepairType> result = new ArrayList<>();
        for (KeyspaceSchedule keyspaceSchedule : myKeyspaceSchedules.values())
        {
            for (Set<TableRepairConfig> tableConfigs : keyspaceSchedule.myTableConfigs.values())
            {
                for (TableRepairConfig tableConfig : tableConfigs)
                {
                    result.add(new TableRepairType(keyspaceSchedule.getKeyspaceName(),
                            tableConfig.getTableName(), tableConfig.getRepairType()));
                }
            }
        }
        return result;
    }

    /** A per-table configured repair type, labelled by keyspace and table. */
    public static final class TableRepairType
    {
        private final String myKeyspace;
        private final String myTable;
        private final com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType myRepairType;

        TableRepairType(final String keyspace, final String table,
                final com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType repairType)
        {
            myKeyspace = keyspace;
            myTable = table;
            myRepairType = repairType;
        }

        /**
         * Get the keyspace name.
         * @return the keyspace.
         */
        public String getKeyspace()
        {
            return myKeyspace;
        }

        /**
         * Get the table name.
         * @return the table.
         */
        public String getTable()
        {
            return myTable;
        }

        /**
         * Get the configured repair type.
         * @return the repair type.
         */
        public RepairType getRepairType()
        {
            return myRepairType;
        }
    }

    static class KeyspaceSchedule
    {
        private String myKeyspaceName;
        private Map<String, Set<TableRepairConfig>> myTableConfigs = new HashMap<>();

        @JsonProperty("name")
        public String getKeyspaceName()
        {
            return myKeyspaceName;
        }

        @JsonProperty("name")
        public void setKeyspaceName(final String keyspaceName)
        {
            myKeyspaceName = keyspaceName;
        }

        @JsonProperty("tables")
        void setTableConfigs(final List<TableRepairConfig> tableRepairConfigs)
        {
            if (tableRepairConfigs != null)
            {
                Map<String, Set<TableRepairConfig>> tableConfigs = new HashMap<>();
                for (TableRepairConfig tableRepairConfig : tableRepairConfigs)
                {
                    tableRepairConfig.validate("Schedule \"" + getKeyspaceName() + "\".\""
                            + tableRepairConfig.getTableName() + "\"");
                    Set<TableRepairConfig> repairConfigs = tableConfigs.getOrDefault(
                            tableRepairConfig.getTableName(), new HashSet<>());
                    repairConfigs.add(tableRepairConfig);
                    tableConfigs.put(tableRepairConfig.getTableName(), repairConfigs);
                }
                myTableConfigs = tableConfigs;
            }
        }

        Set<RepairConfiguration> getRepairConfiguration(final String tableName)
        {
            return findMatching(tableName, myTableConfigs, repairConfig -> repairConfig.stream()
                    .map(TableRepairConfig::asRepairConfiguration).collect(Collectors.toSet()));
        }
    }

    static class TableRepairConfig extends RepairConfig
    {
        private String myTableName;
        private boolean myIsEnabled = true;

        @JsonProperty("name")
        String getTableName()
        {
            return myTableName;
        }

        @JsonProperty("name")
        void setTableName(final String tableName)
        {
            myTableName = tableName;
        }

        @JsonProperty("enabled")
        public boolean isEnabled()
        {
            return myIsEnabled;
        }

        @JsonProperty("enabled")
        public void setEnabled(final boolean enabled)
        {
            myIsEnabled = enabled;
        }

        @Override
        public RepairConfiguration asRepairConfiguration()
        {
            if (isEnabled())
            {
                return super.asRepairConfiguration();
            }

            return RepairConfiguration.DISABLED;
        }
    }

    private static <T, V> Set<V> findMatching(final String searchTerm, final Map<String, T> map,
            final Function<T, Set<V>> function)
    {
        T exactMatch = map.get(searchTerm);
        if (exactMatch != null)
        {
            Set<V> set = function.apply(exactMatch);
            if (!set.isEmpty())
            {
                return set;
            }
        }

        for (Map.Entry<String, T> entry : map.entrySet())
        {
            String regex = entry.getKey();
            if (searchTerm.matches(regex))
            {
                Set<V> set = function.apply(entry.getValue());
                if (!set.isEmpty())
                {
                    return set;
                }
            }
        }

        return Collections.EMPTY_SET;
    }
}

