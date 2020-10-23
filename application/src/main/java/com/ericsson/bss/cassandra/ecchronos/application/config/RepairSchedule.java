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
        KeyspaceSchedule keyspaceSchedule = keyspaces.get(keyspace);

        if (keyspaceSchedule != null)
        {
            return keyspaceSchedule.get(table);
        }

        return Optional.empty();
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
            RepairConfig repairConfig = tables.get(table);

            if (repairConfig != null)
            {
                return Optional.of(repairConfig.asRepairConfiguration());
            }

            return Optional.empty();
        }

        Map<String, TableRepairConfig> getTables()
        {
            return tables;
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

        String getName()
        {
            return name;
        }

        void setName(String name)
        {
            this.name = name;
        }
    }
}
