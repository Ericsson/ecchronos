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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.List;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * A factory that creates a {@link OnDemandRepairJob} based on a {@link TableReference}. The job will deschedule itself
 * automatically when it completes.
 */
public interface OnDemandRepairScheduler
{
    /**
     * Create a repair that is slated to run once for a specified table.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @return A view of the scheduled job.
     * @throws EcChronosException Thrown when the keyspace/table doesn't exist.
     */
    RepairJobView scheduleJob(TableReference tableReference) throws EcChronosException;

    /**
     * Create a repair that is slated to run once for a specified table for all replicas.
     *
     * @param tableReference
     *            The table to schedule a job on.
     * @return A view of the scheduled job.
     * @throws EcChronosException Thrown when the keyspace/table doesn't exist.
     */
    RepairJobView scheduleClusterWideJob(TableReference tableReference) throws EcChronosException;

    List<RepairJobView> getAllClusterWideRepairJobs();

    /**
     * @return the list of all repair jobs.
     */
    List<RepairJobView> getAllRepairJobs();
}
