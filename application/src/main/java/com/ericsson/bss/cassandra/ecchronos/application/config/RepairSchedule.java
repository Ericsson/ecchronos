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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;

public class RepairSchedule
{
    private Map<String, KeyspaceSchedule> keyspaces = new HashMap<>();

    public void setKeyspaces(List<KeyspaceSchedule> keyspaces)
    {
        if (keyspaces != null)
        {
            this.keyspaces = keyspaces.stream().collect(Collectors.toMap(KeyspaceSchedule::getName, ks -> ks));
        }
    }

    public Optional<RepairConfiguration> getRepairConfiguration(String keyspace, String table)
    {
        return findMatching(keyspace, keyspaces, keyspaceSchedule -> keyspaceSchedule.get(table));
    }

    static class KeyspaceSchedule
    {
        private String name;
        private Map<String, TableRepairConfig> tables = new HashMap<>();

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        Optional<RepairConfiguration> get(String table)
        {
            return findMatching(table, tables, repairConfig -> Optional.of(repairConfig.asRepairConfiguration()));
        }

        void setTables(List<TableRepairConfig> tables)
        {
            if (tables != null)
            {
                this.tables = tables.stream().collect(Collectors.toMap(TableRepairConfig::getName, tb -> tb));
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

        void setName(String name)
        {
            this.name = name;
        }

        public boolean isEnabled()
        {
            return enabled;
        }

        public void setEnabled(boolean enabled)
        {
            this.enabled = enabled;
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

    private static <T, V> Optional<V> findMatching(String searchTerm, Map<String, T> map,
            Function<T, Optional<V>> function)
    {
        T exactMatch = map.get(searchTerm);
        if (exactMatch != null)
        {
            Optional<V> optionalValue = function.apply(exactMatch);
            if (optionalValue.isPresent())
            {
                return optionalValue;
            }
        }

        for (Map.Entry<String, T> entry : map.entrySet())
        {
            String regex = entry.getKey();
            if (searchTerm.matches(regex))
            {
                Optional<V> optionalValue = function.apply(entry.getValue());
                if (optionalValue.isPresent())
                {
                    return optionalValue;
                }
            }
        }

        return Optional.empty();
    }
}
