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
package com.ericsson.bss.cassandra.ecchronos.application.config.rest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RestServerConfig
{
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8080;

    private String myHost = DEFAULT_HOST;
    private int myPort = DEFAULT_PORT;

    @JsonProperty("host")
    public final String getHost()
    {
        return myHost;
    }

    @JsonProperty("host")
    public final void setHost(final String host)
    {
        myHost = host;
    }

    @JsonProperty("port")
    public final int getPort()
    {
        return myPort;
    }

    @JsonProperty("port")
    public final void setPort(final int port)
    {
        myPort = port;
    }
}
