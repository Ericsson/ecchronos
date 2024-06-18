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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class DatacenterAwareConfig
{
    private boolean isEnabled = false;

    private Map<String, Datacenter> myDatacenterConfig = new HashMap<>();

    public DatacenterAwareConfig()
    {
    }

    @JsonCreator
    public DatacenterAwareConfig(
        @JsonProperty("enabled") final boolean enabled)
    {
        isEnabled = enabled;
    }

    @JsonProperty("enabled")
    public boolean isEnabled()
    {
        return isEnabled;
    }

    @JsonProperty("datacenters")
    public  Map<String, Datacenter> getDatacenterConfig()
    {
        return myDatacenterConfig;
    }

    @JsonProperty("enabled")
    public void setEnabled(final boolean enabled)
    {
        isEnabled = enabled;
    }

    @JsonProperty("datacenters")
    public void setDatacenterConfig(final List<Datacenter> datacenters)
    {
        if (datacenters != null)
        {
            myDatacenterConfig = datacenters.stream().collect(
                    Collectors.toMap(Datacenter::getName, ks -> ks));
        }
    }

    public static final class Datacenter
    {
        private String myName = "datacenter1";

        private List<Host> myHosts = new ArrayList<>();
        public Datacenter()
        {
        }
        public Datacenter(final String name, final List<Host> hosts)
        {
            myName = name;
            myHosts = hosts;
        }

        @JsonProperty("name")
        public String getName()
        {
            return myName;
        }

        @JsonProperty("hosts")
        public List<Host> getHosts()
        {
            return myHosts;
        }

        @JsonProperty("name")
        public void setName(final String name)
        {
            myName = name;
        }

        @JsonProperty("hosts")
        public void setHosts(final List<Host> hosts)
        {
            if (hosts != null)
            {
                myHosts = hosts;
            }
        }
    }

    public static final class Host
    {
        private static final int DEFAULT_PORT = 9042;
        private String myHost = "localhost";

        private int myPort = DEFAULT_PORT;

        public Host()
        {
        }

        public Host(final String host, final int port)
        {
            myHost = host;
            myPort = port;
        }

        @JsonProperty("host")
        public String getHost()
        {
            return myHost;
        }

        @JsonProperty("port")
        public int getPort()
        {
            return myPort;
        }

        @JsonProperty("host")
        public void setHost(final String host)
        {
            myHost = host;
        }

        @JsonProperty("port")
        public void setPort(final int port)
        {
            myPort = port;
        }
    }
}
