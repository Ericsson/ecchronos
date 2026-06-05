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
package com.ericsson.bss.cassandra.ecchronos.application.config.rest;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Configuration for the embedded REST server. */
public class RestServerConfig
{
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8080;

    private String myHost = DEFAULT_HOST;
    private int myPort = DEFAULT_PORT;

    /** Default constructor. */
    public RestServerConfig()
    {
    }

    /**
     * Returns the host.
     * @return the host
     */
    @JsonProperty("host")
    public final String getHost()
    {
        return myHost;
    }

    /**
     * Sets the host.
     * @param host the hostname
     */
    @JsonProperty("host")
    public final void setHost(final String host)
    {
        myHost = host;
    }

    /**
     * Returns the port.
     * @return the port
     */
    @JsonProperty("port")
    public final int getPort()
    {
        return myPort;
    }

    /**
     * Sets the port.
     * @param port the port number
     */
    @JsonProperty("port")
    public final void setPort(final int port)
    {
        myPort = port;
    }
}
