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
package com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import java.util.List;
import java.util.Set;

/**
 * A factory which takes repair configuration and schedules tables for repair based on the provided configuration.
 *
 * It is the responsibility of the configuration provider to remove the configuration.
 */
public interface RepairScheduler
{
    /**
     * Create or update repair configurations for the specified table.
     *
     * @param node The node to put configurations
     * @param tableReference The table to put configurations for.
     * @param repairConfigurations The new or updated repair configurations.
     */
    void putConfigurations(Node node, TableReference tableReference, Set<RepairConfiguration> repairConfigurations);

    /**
     * Remove repair configuration for the specified table which effectively should remove the schedule.
     *
     * @param node The node to remove configurations
     * @param tableReference The table to remove configuration for.
     */
    void removeConfiguration(Node node, TableReference tableReference);

    /**
     * @return the list of the currently scheduled repair jobs.
     */
    List<ScheduledRepairJobView> getCurrentRepairJobs();

    /**
     * Retrieves the current status of the job being managed by this scheduler.
     * <p>
     * It's intended for monitoring and logging purposes, allowing users to query the job's current state
     * without affecting its execution.
     *
     * @return A {@code String} representing the current status of the job.
     */
    String getCurrentJobStatus();
}


