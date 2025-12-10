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
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;

import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.function.Supplier;

public class DistributedJmxConnection extends Connection<DistributedJmxConnectionProvider>
{
    private RetryPolicyConfig myRetryPolicyConfig = new RetryPolicyConfig();
    private JolokiaConfig myJolokiaConfig = new JolokiaConfig();
    private boolean myReverseDNSResolution = false;
    private Integer myRunDelay;
    private Integer myHeathCheckInterval;

    public DistributedJmxConnection()
    {
        try
        {
            setProvider(AgentJmxConnectionProvider.class);
        }
        catch (NoSuchMethodException ignored)
        {
            // Do something useful ...
        }
    }
    @JsonProperty("runDelay")
    public final Integer getRunDelay()
    {
        return myRunDelay;
    }

    @JsonProperty("runDelay")
    public final void setrunDelay(final Integer runDelay)
    {
        myRunDelay = runDelay;
    }
    @JsonProperty("heathCheckInterval")
    public final Integer getHeathCheckInterval()
    {
        return myHeathCheckInterval;
    }
    @JsonProperty("heathCheckInterval")
    public final void setHeathCheckInterval(final Integer heathCheckInterval)
    {
        myHeathCheckInterval = heathCheckInterval;
    }

    @JsonProperty("retryPolicy")
    public final RetryPolicyConfig getRetryPolicyConfig()
    {
        return myRetryPolicyConfig;
    }

    @JsonProperty("retryPolicy")
    public final void setRetryPolicyConfig(final RetryPolicyConfig retryPolicyConfig)
    {
        myRetryPolicyConfig = retryPolicyConfig;
    }

    @JsonProperty("jolokia")
    public final JolokiaConfig getJolokiaConfig()
    {
        return myJolokiaConfig;
    }

    @JsonProperty("jolokia")
    public final void setJolokiaConfig(final JolokiaConfig jolokiaConfig)
    {
        myJolokiaConfig = jolokiaConfig;
    }

    @JsonProperty("reverseDNSResolution")
    public final void setReseverseDNSResolution(final boolean reverseDNSResolution)
    {
        myReverseDNSResolution = reverseDNSResolution;
    }

    @JsonProperty("reverseDNSResolution")
    public final boolean getReseverseDNSResolution()
    {
        return myReverseDNSResolution;
    }

    /**
     * @return
     */
    @Override
    protected Class<?>[] expectedConstructor()
    {
        return new Class<?>[] {
                                Config.class,
                                Supplier.class,
                                DistributedNativeConnectionProvider.class,
                                EccNodesSync.class,
                                CertificateHandler.class
        };
    }
}
