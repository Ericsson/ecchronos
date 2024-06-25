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

import com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.NoopStatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.application.ReloadingCertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.function.Supplier;

public class NativeConnection extends Connection<NativeConnectionProvider>
{
    private static final int DEFAULT_PORT = 9042;

    private Class<? extends StatementDecorator> myDecoratorClass = NoopStatementDecorator.class;
    private boolean myRemoteRouting = true;
    private AgentConnectionConfig myAgentConnectionConfig = new AgentConnectionConfig();

    public NativeConnection()
    {
        try
        {
            setProvider(DefaultNativeConnectionProvider.class);
            setCertificateHandler(ReloadingCertificateHandler.class);
            setPort(DEFAULT_PORT);
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
    public final void setDatacenterAwareConfig(final AgentConnectionConfig agentConnectionConfig)
    {
        myAgentConnectionConfig = agentConnectionConfig;
    }

    @JsonProperty("decoratorClass")
    public final Class<? extends StatementDecorator> getDecoratorClass()
    {
        return myDecoratorClass;
    }

    @JsonProperty("decoratorClass")
    public final void setDecoratorClass(final Class<StatementDecorator> decoratorClass)
            throws NoSuchMethodException
    {
        decoratorClass.getDeclaredConstructor(Config.class);

        myDecoratorClass = decoratorClass;
    }

    @JsonProperty("remoteRouting")
    public final boolean getRemoteRouting()
    {
        return myRemoteRouting;
    }

    @JsonProperty("remoteRouting")
    public final void setRemoteRouting(final boolean remoteRouting)
    {
        myRemoteRouting = remoteRouting;
    }

    @Override
    protected final Class<?>[] expectedConstructor()
    {
        return new Class<?>[]
                {
                        Config.class, Supplier.class, DefaultRepairConfigurationProvider.class, MeterRegistry.class
                };
    }

    @Override
    public final String toString()
    {
        return String.format("(%s:%d),provider=%s,certificateHandler=%s,decorator=%s",
                getHost(), getPort(), getProviderClass(), getCertificateHandlerClass(), myDecoratorClass);
    }
}
