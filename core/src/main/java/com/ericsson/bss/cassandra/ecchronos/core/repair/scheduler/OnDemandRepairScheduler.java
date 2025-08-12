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

import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.util.List;
import java.util.UUID;

/**
 * A factory that creates a {@link OnDemandRepairJobView} based on a {@link TableReference}. The job will deschedule itself
 * automatically when it completes.
 */
public interface OnDemandRepairScheduler
{
    /**
     * Create a repair that is slated to run once for a specified table.
     *
     * @param tableReference The table to schedule a job on.
     * @param repairType The type of the repair.
     * @param nodeId The ID of the node on which the repair should run.
     * @return A view of the scheduled job.
     * @throws EcChronosException Thrown when the keyspace/table doesn't exist.
     */
    OnDemandRepairJobView scheduleJob(TableReference tableReference,
                                      RepairType repairType,
                                      UUID nodeId) throws EcChronosException;

    /**
     * Create a repair that is slated to run once for a specified table for all replicas.
     *
     * @param tableReference The table to schedule a job on.
     * @param repairType The type of the repair.
     * @return Views of the scheduled job.
     * @throws EcChronosException Thrown when the keyspace/table doesn't exist.
     */
    List<OnDemandRepairJobView> scheduleClusterWideJob(TableReference tableReference,
                                                       RepairType repairType) throws EcChronosException;
    /**
     * Create a repair that is slated to run once for a specified table for all replicas.
     *
     * @param tableReference The table to schedule a job on.
     * @param repairType The type of the repair.
     * @param nodeID The node the repair will run on
     * @return Views of the scheduled job.
     * @throws EcChronosException Thrown when the keyspace/table doesn't exist.
     */
    List<OnDemandRepairJobView> scheduleClusterWideJob(UUID nodeID, TableReference tableReference,
                                                       RepairType repairType) throws EcChronosException;

    /**
     * Retrieves all cluster-wide repair jobs.
     *
     * @return A list of all cluster-wide repair jobs.
     */
    List<OnDemandRepairJobView> getAllClusterWideRepairJobs();

    /**
     * Retrieves all repair jobs for a specific host.
     *
     * @param hostId The ID of the host to retrieve repair jobs for.
     * @return A list of all repair jobs for the specified host.
     */
    List<OnDemandRepairJobView> getAllRepairJobs(UUID hostId);
}
