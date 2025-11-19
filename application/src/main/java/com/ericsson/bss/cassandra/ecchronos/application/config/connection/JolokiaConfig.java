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

/**
 * Configuration class for Jolokia integration with JMX.
 * This class allows enabling or disabling the Jolokia adapter and specifying the port to use.
 */
public class JolokiaConfig
{
    private static final int DEFAULT_JOLOKIA_PORT = 8778;
    private boolean myEnabled = false;
    private int myPort = DEFAULT_JOLOKIA_PORT;
    private boolean myUsePem = false;

    /**
     * Sets whether the Jolokia adapter is enabled.
     *
     * @param enabled {@code true} to enable the Jolokia adapter; {@code false} otherwise.
     */
    @JsonProperty("enabled")
    public void setEnabled(final boolean enabled)
    {
        myEnabled = enabled;
    }

    /**
     * Sets the port to be used by the Jolokia adapter.
     *
     * @param port the port number for the Jolokia adapter.
     */
    @JsonProperty("port")
    public void setDefaultJolokiaPort(final int port)
    {
        myPort = port;
    }

    /**
     * Returns whether the Jolokia adapter is enabled.
     *
     * @return {@code true} if the Jolokia adapter is enabled; {@code false} otherwise.
     */
    @JsonProperty("enabled")
    public boolean isEnabled()
    {
        return myEnabled;
    }

    /**
     * Returns the port used by the Jolokia adapter.
     *
     * @return the port number for the Jolokia adapter.
     */
    @JsonProperty("port")
    public int getPort()
    {
        return myPort;
    }

    /**
     * Sets whether the PEM certificate is enabled.
     *
     * @param enabled {@code true} to enable PEM certificate usage; {@code false} otherwise.
     */
    @JsonProperty("usePem")
    public void setUsePem(final boolean usePem)
    {
        myUsePem = usePem;
    }

    /**
     * Returns whether the PEM certificates usage is enabled.
     *
     * @return {@code true} if the PEM certificates is enabled; {@code false} otherwise.
     */
    @JsonProperty("usePem")
    public boolean usePem()
    {
        return myUsePem;
    }
}
