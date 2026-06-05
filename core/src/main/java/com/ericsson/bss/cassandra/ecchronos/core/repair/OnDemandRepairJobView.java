/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.UUID;

/**
 * Read-only view of an on-demand repair job.
 *
 * @param id the job identifier
 * @param hostId the host identifier
 * @param tableReference the table reference
 * @param status the job status
 * @param progress the job progress
 * @param completionTime the completion time
 * @param repairType the repair type
 */
public record OnDemandRepairJobView(UUID id,
                                    UUID hostId,
                                    TableReference tableReference,
                                    Status status,
                                    double progress,
                                    long completionTime,
                                    RepairOptions.RepairType repairType)
{
    /** Represents the possible states of this job. */
    public enum Status
    {
        /** Completed status. */
        COMPLETED,
        /** In queue status. */
        IN_QUEUE,
        /** Warning status. */
        WARNING,
        /** Error status. */
        ERROR,
        /** Blocked status. */
        BLOCKED
    }
}
