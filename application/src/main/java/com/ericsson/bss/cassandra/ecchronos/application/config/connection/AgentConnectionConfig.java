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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class AgentConnectionConfig
{
    private boolean enabled = false;
    private ConnectionType myType = ConnectionType.datacenterAware;
    private Map<String, Host> myContactPoints = new HashMap<>();
    private DatacenterAware myDatacenterAware = new DatacenterAware();
    private RackAware myRackAware = new RackAware();
    private HostAware myHostAware = new HostAware();

    public AgentConnectionConfig()
    {

    }

    @JsonCreator
    public AgentConnectionConfig(
        @JsonProperty("enabled") final boolean enableAgent)
    {
        enabled = enableAgent;
    }

    @JsonProperty("enabled")
    public boolean isEnabled()
    {
        return enabled;
    }

    @JsonProperty("enabled")
    public void setEnabled(final boolean enableAgent)
    {
        enabled = enableAgent;
    }

    @JsonProperty("type")
    public ConnectionType getType()
    {
        return myType;
    }

    @JsonProperty("type")
    public void setType(final String type)
    {
        myType = ConnectionType.valueOf(type);
    }

    @JsonProperty("contactPoints")
    public Map<String, Host> getContactPoints()
    {
        return myContactPoints;
    }

    @JsonProperty("contactPoints")
    public void setMyContactPoints(final List<Host> contactPoints)
    {
        if (contactPoints != null)
        {
            myContactPoints = contactPoints.stream().collect(
                    Collectors.toMap(Host::getHost, ks -> ks));
        }
    }

    @JsonProperty("datacenterAware")
    public void setDatacenterAware(final DatacenterAware datacenterAware)
    {
        myDatacenterAware = datacenterAware;
    }

    @JsonProperty("datacenterAware")
    public DatacenterAware getDatacenterAware()
    {
        return myDatacenterAware;
    }

    @JsonProperty("rackAware")
    public void setRackAware(final RackAware rackAware)
    {
        myRackAware = rackAware;
    }

    @JsonProperty("rackAware")
    public RackAware getRackAware()
    {
        return myRackAware;
    }

    @JsonProperty("hostAware")
    public void setHostAware(final HostAware hostAware)
    {
        myHostAware = hostAware;
    }

    @JsonProperty("hostAware")
    public HostAware getHostAware()
    {
        return myHostAware;
    }

    public enum ConnectionType
    {
        datacenterAware, rackAware, hostAware
    }

    public static final class DatacenterAware
    {
        private Map<String, Datacenter> myDatacenters = new HashMap<>();

        public DatacenterAware()
        {

        }

        @JsonProperty("datacenters")
        public Map<String, Datacenter> getDatacenterAware()
        {
            return myDatacenters;
        }

        @JsonProperty("datacenters")
        public void setDatacenters(final List<Datacenter> datacenters)
        {
            if (datacenters != null)
            {
                myDatacenters = datacenters.stream().collect(
                        Collectors.toMap(Datacenter::getName, ks -> ks));
            }
        }

        public static final class Datacenter
        {
            private String myName;

            public Datacenter()
            {

            }

            public Datacenter(final String name)
            {
                myName = name;
            }

            @JsonProperty("name")
            public String getName()
            {
                return myName;
            }

            @JsonProperty("name")
            public void setName(final String name)
            {
                myName = name;
            }
        }
    }

    public static final class RackAware
    {
        private Map<String, Rack> myRackAware = new HashMap<>();

        public RackAware()
        {

        }

        @JsonProperty("racks")
        public Map<String, Rack> getRackAware()
        {
            return myRackAware;
        }

        @JsonProperty("racks")
        public void setMyRackAware(final List<Rack> rackAware)
        {
            if (rackAware != null)
            {
                myRackAware = rackAware.stream().collect(
                        Collectors.toMap(Rack::getRackName, ks -> ks));
            }
        }

        public static final class Rack
        {
            private String myDatacenterName;
            private String myRackName;

            public Rack()
            {

            }

            public Rack(final String datacenterName, final String rackName)
            {
                myDatacenterName = datacenterName;
                myRackName = rackName;
            }

            @JsonProperty("datacenterName")
            public String getDatacenterName()
            {
                return myDatacenterName;
            }

            @JsonProperty("datacenterName")
            public void setName(final String datacenterName)
            {
                myDatacenterName = datacenterName;
            }

            @JsonProperty("rackName")
            public String getRackName()
            {
                return myRackName;
            }

            @JsonProperty("rackName")
            public void setRack(final String rackName)
            {
                myRackName = rackName;
            }
        }
    }

    public static final class HostAware
    {
        private Map<String, Host> myHosts = new HashMap<>();

        public HostAware()
        {

        }

        @JsonProperty("hosts")
        public Map<String, Host> getHosts()
        {
            return myHosts;
        }

        @JsonProperty("hosts")
        public void setHosts(final List<Host> hosts)
        {
            if (hosts != null)
            {
                myHosts = hosts.stream().collect(
                        Collectors.toMap(Host::getHost, ks -> ks));
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
