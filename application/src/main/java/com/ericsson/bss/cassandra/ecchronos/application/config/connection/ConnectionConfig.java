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

import com.ericsson.bss.cassandra.ecchronos.application.config.Interval;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectionConfig
{
    private DistributedNativeConnection myCqlConnection = new DistributedNativeConnection();
    private DistributedJmxConnection myJmxConnection = new DistributedJmxConnection();
    private Interval myConnectionDelay;

    @JsonProperty("cql")
    public final DistributedNativeConnection getCqlConnection()
    {
        return myCqlConnection;
    }

    @JsonProperty("jmx")
    public final DistributedJmxConnection getJmxConnection()
    {
        return myJmxConnection;
    }

    @JsonProperty("cql")
    public final void setCqlConnection(final DistributedNativeConnection cqlConnection)
    {
        if (cqlConnection != null)
        {
            myCqlConnection = cqlConnection;
        }
    }

    @JsonProperty("jmx")
    public final void setJmxConnection(final DistributedJmxConnection jmxConnection)
    {
        if (jmxConnection != null)
        {
            myJmxConnection = jmxConnection;
        }
    }

    @Override
    public final String toString()
    {
        return String.format("Connection(cql=%s, jmx=%s)", myCqlConnection, myJmxConnection);
    }
    /**
     * Sets the connectionDelay used to specify the time until the next connection.
     *
     * @param connectionDelay
     *         the local datacenter to set.
     */
    @JsonProperty("connectionDelay")
    public void setConnectionDelay(final Interval connectionDelay)
    {
        myConnectionDelay = connectionDelay;
    }
    /**
     * Gets the connectionDelay used to specify the time until the next connection.
     *
     * @return the connectionDelay.
     */
    @JsonProperty("connectionDelay")
    public Interval getConnectionDelay()
    {
        return myConnectionDelay;
    }

}
