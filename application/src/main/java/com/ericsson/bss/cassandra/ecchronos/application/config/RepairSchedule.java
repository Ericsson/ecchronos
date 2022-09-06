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

    public final void setKeyspaces(final List<KeyspaceSchedule> theKeyspaces)
    {
        if (theKeyspaces != null)
        {
            this.keyspaces = theKeyspaces.stream().collect(Collectors.toMap(KeyspaceSchedule::getName, ks -> ks));
        }
    }

    public final Optional<RepairConfiguration> getRepairConfiguration(final String keyspace, final String table)
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

        public void setName(final String nameValue)
        {
            this.name = nameValue;
        }

        Optional<RepairConfiguration> get(final String table)
        {
            return findMatching(table, tables, repairConfig -> Optional.of(repairConfig.asRepairConfiguration()));
        }

        void setTables(final List<TableRepairConfig> theTables)
        {
            if (theTables != null)
            {
                this.tables = theTables.stream().collect(Collectors.toMap(TableRepairConfig::getName, tb -> tb));
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

    private static <T, V> Optional<V> findMatching(final String searchTerm,
                                                   final Map<String, T> map,
                                                   final Function<T, Optional<V>> function)
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
