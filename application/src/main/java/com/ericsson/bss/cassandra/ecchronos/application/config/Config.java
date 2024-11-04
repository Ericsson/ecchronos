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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.LockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.rest.RestServerConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.runpolicy.RunPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.scheduler.SchedulerConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Config
{
    private ConnectionConfig myConnectionConfig = new ConnectionConfig();
    private GlobalRepairConfig myRepairConfig = new GlobalRepairConfig();
    private RunPolicyConfig myRunPolicyConfig = new RunPolicyConfig();
    private SchedulerConfig mySchedulerConfig = new SchedulerConfig();
    private RestServerConfig myRestServerConfig = new RestServerConfig();
    private LockFactoryConfig myLockFactoryConfig = new LockFactoryConfig();

    @JsonProperty("connection")
    public final ConnectionConfig getConnectionConfig()
    {
        return myConnectionConfig;
    }

    @JsonProperty("connection")
    public final void setConnectionConfig(final ConnectionConfig connectionConfig)
    {
        if (connectionConfig != null)
        {
            myConnectionConfig = connectionConfig;
        }
    }

    /**
     * Get the global repair configuration.
     *
     * @return GlobalRepairConfig
     */
    @JsonProperty("repair")
    public GlobalRepairConfig getRepairConfig()
    {
        return myRepairConfig;
    }

    /**
     * Set repair configuration.
     *
     * @param repairConfig The repair configuration.
     */
    @JsonProperty("repair")
    public void setRepairConfig(final GlobalRepairConfig repairConfig)
    {
        if (repairConfig != null)
        {
            myRepairConfig = repairConfig;
            myRepairConfig.validate("Global");
        }
    }

    @JsonProperty("run_policy")
    public final RunPolicyConfig getRunPolicy()
    {
        return myRunPolicyConfig;
    }

    @JsonProperty("run_policy")
    public final void setRunPolicyConfig(final RunPolicyConfig runPolicyConfig)
    {
        if (runPolicyConfig != null)
        {
            myRunPolicyConfig = runPolicyConfig;
        }
    }

    @JsonProperty("scheduler")
    public final SchedulerConfig getSchedulerConfig()
    {
        return mySchedulerConfig;
    }

    /**
     * Set the scheduler.
     *
     * @param schedulerConfig The scheduler.
     */
    @JsonProperty("scheduler")
    public void setSchedulerConfig(final SchedulerConfig schedulerConfig)
    {
        if (schedulerConfig != null)
        {
            mySchedulerConfig = schedulerConfig;
        }
    }

    @JsonProperty("rest_server")
    public final RestServerConfig getRestServer()
    {
        return myRestServerConfig;
    }

    @JsonProperty("rest_server")
    public final void setRestServerConfig(final RestServerConfig restServerConfig)
    {
        if (restServerConfig != null)
        {
            myRestServerConfig = restServerConfig;
        }
    }

    @JsonProperty("lock_factory")
    public final LockFactoryConfig getLockFactory()
    {
        return myLockFactoryConfig;
    }

    @JsonProperty("lock_factory")
    public final void setLockFactoryConfig(final LockFactoryConfig lockFactoryConfig)
    {
        if (lockFactoryConfig != null)
        {
            myLockFactoryConfig = lockFactoryConfig;
        }
    }
}
