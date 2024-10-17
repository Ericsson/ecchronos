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

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.function.Supplier;

public class DistributedNativeConnection extends Connection<DistributedNativeConnectionProvider>
{
    private AgentConnectionConfig myAgentConnectionConfig = new AgentConnectionConfig();
    private Interval myConnectionDelay = new Interval();

    public DistributedNativeConnection()
    {
        try
        {
            setProvider(AgentNativeConnectionProvider.class);
        }
        catch (NoSuchMethodException ignored)
        {
            // Do something useful ...
        }
    }

    @JsonProperty("agent")
    public final AgentConnectionConfig getAgentConnectionConfig()
    {
        return myAgentConnectionConfig;
    }

    @JsonProperty("agent")
    public final void setAgentConnectionConfig(final AgentConnectionConfig agentConnectionConfig)
    {
        myAgentConnectionConfig = agentConnectionConfig;
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

    /**
     * @return Class<?>[]
     */
    @Override
    protected Class<?>[] expectedConstructor()
    {
        return new Class<?>[]
                {
                        Config.class,
                        Supplier.class,
                        CertificateHandler.class,
                        DefaultRepairConfigurationProvider.class,
                };
    }
}
