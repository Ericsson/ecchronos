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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;

public class RepairSchedule
{
    private Map<String, KeyspaceSchedule> keyspaces = new HashMap<>();

    public final void setKeyspaces(final List<KeyspaceSchedule> theKeyspaces)
    {
        if (theKeyspaces != null)
        {
            this.keyspaces = theKeyspaces.stream().collect(Collectors.toMap(KeyspaceSchedule::getName, ks -> ks));
        }
    }

    public final Set<RepairConfiguration> getRepairConfigurations(final String keyspace, final String table)
    {
        return findMatching(keyspace, keyspaces, keyspaceSchedule -> keyspaceSchedule.get(table));
    }

    static class KeyspaceSchedule
    {
        private String name;
        private Map<String, Set<TableRepairConfig>> tables = new HashMap<>();

        public String getName()
        {
            return name;
        }

        public void setName(final String nameValue)
        {
            this.name = nameValue;
        }

        Set<RepairConfiguration> get(final String table)
        {
            return findMatching(table, tables, repairConfig -> repairConfig.stream()
                    .map(TableRepairConfig::asRepairConfiguration).collect(Collectors.toSet()));
        }

        void setTables(final List<TableRepairConfig> theTables)
        {
            if (theTables != null)
            {
                Map<String, Set<TableRepairConfig>> tableConfigs = new HashMap<>();
                for (TableRepairConfig table : theTables)
                {
                    Set<TableRepairConfig> repairConfigs = tableConfigs.getOrDefault(table.getName(), new HashSet<>());
                    repairConfigs.add(table);
                    tableConfigs.put(table.getName(), repairConfigs);
                }
                this.tables = tableConfigs;
            }
        }
    }

    static class TableRepairConfig extends RepairConfig
    {
        private String name;
        private boolean enabled = true;

        String getName()
        {
            return name;
        }

        void setName(final String nameValue)
        {
            this.name = nameValue;
        }

        public boolean isEnabled()
        {
            return enabled;
        }

        public void setEnabled(final boolean enabledValue)
        {
            this.enabled = enabledValue;
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

    private static <T, V> Set<V> findMatching(final String searchTerm,
                                                   final Map<String, T> map,
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
