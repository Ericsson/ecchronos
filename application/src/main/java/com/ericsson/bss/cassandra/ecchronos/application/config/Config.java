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

import com.ericsson.bss.cassandra.ecchronos.application.config.connection.ConnectionConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.LockFactoryConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.metrics.StatisticsConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.GlobalRepairConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.rest.RestServerConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.runpolicy.RunPolicyConfig;
import com.ericsson.bss.cassandra.ecchronos.application.config.scheduler.SchedulerConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Root configuration model for the ecChronos application. */
public class Config
{
    private ConnectionConfig myConnectionConfig = new ConnectionConfig();
    private GlobalRepairConfig myRepairConfig = new GlobalRepairConfig();
    private StatisticsConfig myStatisticsConfig = new StatisticsConfig();
    private LockFactoryConfig myLockFactoryConfig = new LockFactoryConfig();
    private RunPolicyConfig myRunPolicyConfig = new RunPolicyConfig();
    private SchedulerConfig mySchedulerConfig = new SchedulerConfig();
    private RestServerConfig myRestServerConfig = new RestServerConfig();

    /** Default constructor. */
    public Config()
    {
    }

    /**
     * Returns the connection config.
     * @return the connection config
     */
    @JsonProperty("connection")
    public final ConnectionConfig getConnectionConfig()
    {
        return myConnectionConfig;
    }

    /**
     * Sets the connection config.
     * @param connectionConfig the connection config
     */
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
     * @param globalRepairConfig The repair configuration.
     */
    @JsonProperty("repair")
    public void setRepairConfig(final GlobalRepairConfig globalRepairConfig)
    {
        if (globalRepairConfig != null)
        {
            myRepairConfig = globalRepairConfig;
            myRepairConfig.validate("Global");
        }
    }

    /**
     * Returns the statistics config.
     * @return the statistics config
     */
    @JsonProperty("statistics")
    public final StatisticsConfig getStatisticsConfig()
    {
        return myStatisticsConfig;
    }

    /**
     * Sets the statistics config.
     * @param statisticsConfig the statistics config
     */
    @JsonProperty("statistics")
    public final void setStatisticsConfig(final StatisticsConfig statisticsConfig)
    {
        if (statisticsConfig != null)
        {
            myStatisticsConfig = statisticsConfig;
            myStatisticsConfig.validate();
        }
    }

    /**
     * Returns the lock factory.
     * @return the lock factory
     */
    @JsonProperty("lock_factory")
    public final LockFactoryConfig getLockFactory()
    {
        return myLockFactoryConfig;
    }

    /**
     * Sets the lock factory config.
     * @param lockFactoryConfig the lock factory config
     */
    @JsonProperty("lock_factory")
    public final void setLockFactoryConfig(final LockFactoryConfig lockFactoryConfig)
    {
        if (lockFactoryConfig != null)
        {
            myLockFactoryConfig = lockFactoryConfig;
        }
    }

    /**
     * Returns the run policy.
     * @return the run policy
     */
    @JsonProperty("run_policy")
    public final RunPolicyConfig getRunPolicy()
    {
        return myRunPolicyConfig;
    }

    /**
     * Sets the run policy config.
     * @param runPolicyConfig the run policy config
     */
    @JsonProperty("run_policy")
    public final void setRunPolicyConfig(final RunPolicyConfig runPolicyConfig)
    {
        if (runPolicyConfig != null)
        {
            myRunPolicyConfig = runPolicyConfig;
        }
    }

    /**
     * Returns the scheduler config.
     * @return the scheduler config
     */
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

    /**
     * Returns the REST server.
     * @return the REST server
     */
    @JsonProperty("rest_server")
    public final RestServerConfig getRestServer()
    {
        return myRestServerConfig;
    }

    /**
     * Sets the REST server config.
     * @param restServerConfig the REST server config
     */
    @JsonProperty("rest_server")
    public final void setRestServerConfig(final RestServerConfig restServerConfig)
    {
        if (restServerConfig != null)
        {
            myRestServerConfig = restServerConfig;
        }
    }

}
