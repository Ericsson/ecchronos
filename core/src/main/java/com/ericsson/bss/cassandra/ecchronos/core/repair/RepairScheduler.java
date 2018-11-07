/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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

/**
 * A factory which takes repair configuration and schedules tables for repair based on the provided configuration.
 *
 * It is the responsibility of the configuration provider to remove the configuration.
 */
public interface RepairScheduler
{
    /**
     * Create or update repair configuration for the specified table.
     *
     * @param tableReference The table to put configuration for.
     * @param repairConfiguration The new or updated repair configuration.
     */
    void putConfiguration(TableReference tableReference, RepairConfiguration repairConfiguration);

    /**
     * Remove repair configuration for the specified table which effectively should remove the schedule.
     *
     * @param tableReference The table to remove configuration for.
     */
    void removeConfiguration(TableReference tableReference);
}
