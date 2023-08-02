/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

public class ConnectionConfig
{
    private NativeConnection myCqlConnection = new NativeConnection();
    private JmxConnection myJmxConnection = new JmxConnection();

    @JsonProperty("cql")
    public final NativeConnection getCqlConnection()
    {
        return myCqlConnection;
    }

    @JsonProperty("jmx")
    public final JmxConnection getJmxConnection()
    {
        return myJmxConnection;
    }

    @JsonProperty("cql")
    public final void setCqlConnection(final NativeConnection cqlConnection)
    {
        if (cqlConnection != null)
        {
            myCqlConnection = cqlConnection;
        }
    }

    @JsonProperty("jmx")
    public final void setJmxConnection(final JmxConnection jmxConnection)
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
}
