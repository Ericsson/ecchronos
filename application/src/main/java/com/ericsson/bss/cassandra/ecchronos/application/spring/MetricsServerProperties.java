/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.application.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.web.server.Ssl;

/** Properties for the metrics server endpoint. */
@ConfigurationProperties (prefix = "metrics-server")
public class MetricsServerProperties
{
    private static final int DEFAULT_PORT = 8081;

    private boolean enabled = false;
    private int port = DEFAULT_PORT;

    @NestedConfigurationProperty
    private Ssl ssl;

    /** Default constructor. */
    public MetricsServerProperties()
    {
    }

    /**
     * Returns whether enabled.
     * @return true if enabled
     */
    public final boolean isEnabled()
    {
        return enabled;
    }

    /**
     * Sets the enabled.
     * @param isEnabled the is enabled
     */
    public final void setEnabled(final boolean isEnabled)
    {
        this.enabled = isEnabled;
    }

    /**
     * Returns the port.
     * @return the port
     */
    public final int getPort()
    {
        return port;
    }

    /**
     * Sets the port.
     * @param aPort the port
     */
    public final void setPort(final int aPort)
    {
        this.port = aPort;
    }

    /**
     * Returns the SSL.
     * @return the SSL
     */
    public final Ssl getSsl()
    {
        return ssl;
    }

    /**
     * Sets the SSL.
     * @param theSsl the SSL
     */
    public final void setSsl(final Ssl theSsl)
    {
        this.ssl = theSsl;
    }
}
