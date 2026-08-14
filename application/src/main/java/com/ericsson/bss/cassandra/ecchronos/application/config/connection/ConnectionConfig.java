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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration holder for CQL and JMX connection settings. */
public class ConnectionConfig
{
    private ThreadPoolTaskConfig myThreadPoolTaskConfig = new ThreadPoolTaskConfig();
    private DistributedNativeConnection myCqlConnection = new DistributedNativeConnection();
    private DistributedJmxConnection myJmxConnection = new DistributedJmxConnection();

    /** Default constructor. */
    public ConnectionConfig()
    {
    }

    /**
     * Returns the CQL connection.
     * @return the CQL connection
     */
    @JsonProperty("cql")
    public final DistributedNativeConnection getCqlConnection()
    {
        return myCqlConnection;
    }

    /**
     * Returns the JMX connection.
     * @return the JMX connection
     */
    @JsonProperty("jmx")
    public final DistributedJmxConnection getJmxConnection()
    {
        return myJmxConnection;
    }

    /**
     * Sets the CQL connection.
     * @param cqlConnection the CQL connection
     */
    @JsonProperty("cql")
    public final void setCqlConnection(final DistributedNativeConnection cqlConnection)
    {
        if (cqlConnection != null)
        {
            myCqlConnection = cqlConnection;
        }
    }

    /**
     * Sets the JMX connection.
     * @param jmxConnection the JMX connection
     */
    @JsonProperty("jmx")
    public final void setJmxConnection(final DistributedJmxConnection jmxConnection)
    {
        if (jmxConnection != null)
        {
            myJmxConnection = jmxConnection;
        }
    }

    /**
     * Returns the thread pool task config.
     * @return the thread pool task config
     */
    @JsonProperty("threadPool")
    public final ThreadPoolTaskConfig getThreadPoolTaskConfig()
    {
        return myThreadPoolTaskConfig;
    }

    /**
     * Sets the thread pool task config.
     * @param threadPoolTaskConfig the thread pool task config
     */
    @JsonProperty("threadPool")
    public final void setThreadPoolTaskConfig(final ThreadPoolTaskConfig threadPoolTaskConfig)
    {
        myThreadPoolTaskConfig = threadPoolTaskConfig;
    }

    @Override
    public final String toString()
    {
        return String.format("Connection(cql=%s, jmx=%s)", myCqlConnection, myJmxConnection);
    }
}
