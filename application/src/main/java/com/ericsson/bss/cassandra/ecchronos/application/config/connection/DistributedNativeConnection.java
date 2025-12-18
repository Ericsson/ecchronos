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
import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.connection.ConnectionType;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DistributedNativeConnection extends Connection<DistributedNativeConnectionProvider>
{
    private Interval myConnectionDelay = new Interval();
    private Timeout myTimeout = new Timeout();
    private CQLRetryPolicyConfig myCqlRetryPolicy = new CQLRetryPolicyConfig();
    private ConnectionType myType = ConnectionType.datacenterAware;
    private String myLocalDatacenter = "datacenter1";
    private Map<String, Host> myContactPoints = new HashMap<>();
    private DatacenterAware myDatacenterAware = new DatacenterAware();
    private RackAware myRackAware = new RackAware();
    private HostAware myHostAware = new HostAware();
    private String myInstanceName;
    private RetryPolicyConfig.RetrySchedule myRetrySchedule = new RetryPolicyConfig.RetrySchedule();

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

    @JsonProperty("timeout")
    public final Timeout getTimeout()
    {
        return myTimeout;
    }

    @JsonProperty("timeout")
    public final void setTimeout(final Timeout timeout)
    {
        myTimeout = timeout;
    }

    /**
     * Gets the retry policy configuration.
     *
     * @return the retry policy configuration.
     */
    @JsonProperty("retryPolicy")
    public CQLRetryPolicyConfig getCqlRetryPolicy()
    {
        return myCqlRetryPolicy;
    }

    /**
     * Sets the retry policy configuration.
     *
     * @param cqlRetryPolicyConfig
     *         the retry policy configuration to set.
     */
    @JsonProperty("retryPolicy")
    public void setCqlRetryPolicy(final CQLRetryPolicyConfig cqlRetryPolicyConfig)
    {
        myCqlRetryPolicy = cqlRetryPolicyConfig;
    }

    @Deprecated
    @JsonProperty("remoteRouting")
    public void setRemoteRouting(final boolean remoteRouting)
    {
        // noop
    }

    /**
     * Gets unique ecchronos instance name.
     *
     * @return the instance name.
     */
    @JsonProperty("instanceName")
    public String getInstanceName()
    {
        return myInstanceName;
    }

    /**
     * Sets unique instance name.
     *
     * @param  instanceName
     *         the instance name as a string.
     * @throws ConfigurationException
     *         if the provided instance name is invalid.
     */
    @JsonProperty("instanceName")
    public void setInstanceName(final String instanceName) throws ConfigurationException
    {
        if (instanceName == null || instanceName.isBlank())
        {
            throw new ConfigurationException(
                    "Invalid instance name: "
                            +
                            instanceName
                            +
                            "\nInstance name must not be null or empty");
        }
        myInstanceName = instanceName;
    }

    /**
     * Gets the connection type.
     *
     * @return the connection type.
     */
    @JsonProperty("type")
    public ConnectionType getType()
    {
        return myType;
    }

    /**
     * Sets the connection type.
     *
     * @param type
     *         the connection type as a string.
     * @throws ConfigurationException
     *         if the provided type is invalid.
     */
    @JsonProperty("type")
    public void setType(final String type) throws ConfigurationException
    {
        try
        {
            myType = ConnectionType.valueOf(type);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(
                    "Invalid connection type: "
                            +
                            type
                            +
                            "\nAccepted configurations are: datacenterAware, rackAware, hostAware", e);
        }
    }

    /**
     * Sets the local datacenter used for load-balancing policy.
     *
     * @param localDatacenter
     *         the local datacenter to set.
     */
    @JsonProperty("localDatacenter")
    public void setLocalDatacenter(final String localDatacenter)
    {
        myLocalDatacenter = localDatacenter;
    }

    /**
     * Gets the local datacenter used for load-balancing policy.
     *
     * @return the local datacenter.
     */
    @JsonProperty("localDatacenter")
    public String getLocalDatacenter()
    {
        return myLocalDatacenter;
    }

    /**
     * Gets the contact points.
     *
     * @return the contact points map.
     */
    @JsonProperty("contactPoints")
    public Map<String, Host> getContactPoints()
    {
        return myContactPoints;
    }

    /**
     * Sets the contact points.
     *
     * @param contactPoints
     *         a list of contact points.
     */
    @JsonProperty("contactPoints")
    public void setContactPoints(final List<Host> contactPoints)
    {
        if (contactPoints != null)
        {
            myContactPoints = contactPoints.stream().collect(
                    Collectors.toMap(Host::getHost, ks -> ks));
        }
    }

    /**
     * Sets the datacenter-aware configuration.
     *
     * @param datacenterAware
     *         the datacenter-aware configuration to set.
     */
    @JsonProperty("datacenterAware")
    public void setDatacenterAware(final DatacenterAware datacenterAware)
    {
        myDatacenterAware = datacenterAware;
    }

    /**
     * Gets the datacenter-aware configuration.
     *
     * @return the datacenter-aware configuration.
     */
    @JsonProperty("datacenterAware")
    public DatacenterAware getDatacenterAware()
    {
        return myDatacenterAware;
    }

    /**
     * Sets the rack-aware configuration.
     *
     * @param rackAware
     *         the rack-aware configuration to set.
     */
    @JsonProperty("rackAware")
    public void setRackAware(final RackAware rackAware)
    {
        myRackAware = rackAware;
    }

    /**
     * Gets the rack-aware configuration.
     *
     * @return the rack-aware configuration.
     */
    @JsonProperty("rackAware")
    public RackAware getRackAware()
    {
        return myRackAware;
    }

    /**
     * Sets the host-aware configuration.
     *
     * @param hostAware
     *         the host-aware configuration to set.
     */
    @JsonProperty("hostAware")
    public void setHostAware(final HostAware hostAware)
    {
        myHostAware = hostAware;
    }

    /**
     * Gets the host-aware configuration.
     *
     * @return the host-aware configuration.
     */
    @JsonProperty("hostAware")
    public HostAware getHostAware()
    {
        return myHostAware;
    }

    @JsonProperty("reloadSchedule")
    public final void setReloadSchedule(final RetryPolicyConfig.RetrySchedule retrySchedule)
    {
        myRetrySchedule = retrySchedule;
    }

    @JsonProperty("reloadSchedule")
    public final RetryPolicyConfig.RetrySchedule getReloadSchedule()
    {
        return myRetrySchedule;
    }

    /**
     * Configuration for datacenter-aware connections.
     */
    public static final class DatacenterAware
    {
        private Map<String, DatacenterAware.Datacenter> myDatacenters = new HashMap<>();

        /**
         * Default constructor for DatacenterAware.
         */
        public DatacenterAware()
        {

        }

        /**
         * Gets the datacenters map.
         *
         * @return the datacenters map.
         */
        @JsonProperty("datacenters")
        public Map<String, DatacenterAware.Datacenter> getDatacenters()
        {
            return myDatacenters;
        }

        /**
         * Sets the datacenters.
         *
         * @param datacenters
         *         a list of datacenters.
         */
        @JsonProperty("datacenters")
        public void setDatacenters(final List<DatacenterAware.Datacenter> datacenters)
        {
            if (datacenters != null)
            {
                myDatacenters = datacenters.stream().collect(
                        Collectors.toMap(DatacenterAware.Datacenter::getName, ks -> ks));
            }
        }

        /**
         * Represents a datacenter.
         */
        public static final class Datacenter
        {
            private String myName;

            /**
             * Default constructor for Datacenter.
             */
            public Datacenter()
            {

            }

            /**
             * Constructor with name.
             *
             * @param name
             *         the name of the datacenter.
             */
            public Datacenter(final String name)
            {
                myName = name;
            }

            /**
             * Gets the name of the datacenter.
             *
             * @return the name of the datacenter.
             */
            @JsonProperty("name")
            public String getName()
            {
                return myName;
            }

            /**
             * Sets the name of the datacenter.
             *
             * @param name
             *         the name to set.
             */
            @JsonProperty("name")
            public void setName(final String name)
            {
                myName = name;
            }
        }
    }

    /**
     * Configuration for rack-aware connections.
     */
    public static final class RackAware
    {
        private Map<String, RackAware.Rack> myRackAware = new HashMap<>();

        /**
         * Default constructor for RackAware.
         */
        public RackAware()
        {

        }

        /**
         * Gets the racks map.
         *
         * @return the racks map.
         */
        @JsonProperty("racks")
        public Map<String, RackAware.Rack> getRacks()
        {
            return myRackAware;
        }

        /**
         * Sets the racks.
         *
         * @param rackAware
         *         a list of racks.
         */
        @JsonProperty("racks")
        public void setRacks(final List<RackAware.Rack> rackAware)
        {
            if (rackAware != null)
            {
                myRackAware = rackAware.stream().collect(
                        Collectors.toMap(RackAware.Rack::getRackName, ks -> ks));
            }
        }

        /**
         * Represents a rack with a datacenter name and rack name.
         */
        public static final class Rack
        {
            private String myDatacenterName;
            private String myRackName;

            /**
             * Default constructor for Rack.
             */
            public Rack()
            {

            }

            /**
             * Constructor with datacenter name and rack name.
             *
             * @param datacenterName
             *         the datacenter name.
             * @param rackName
             *         the rack name.
             */
            public Rack(final String datacenterName, final String rackName)
            {
                myDatacenterName = datacenterName;
                myRackName = rackName;
            }

            /**
             * Gets the datacenter name.
             *
             * @return the datacenter name.
             */
            @JsonProperty("datacenterName")
            public String getDatacenterName()
            {
                return myDatacenterName;
            }

            /**
             * Sets the datacenter name.
             *
             * @param datacenterName
             *         the datacenter name to set.
             */
            @JsonProperty("datacenterName")
            public void setDatacenterName(final String datacenterName)
            {
                myDatacenterName = datacenterName;
            }

            /**
             * Gets the rack name.
             *
             * @return the rack name.
             */
            @JsonProperty("rackName")
            public String getRackName()
            {
                return myRackName;
            }

            /**
             * Sets the rack name.
             *
             * @param rackName
             *         the rack name to set.
             */
            @JsonProperty("rackName")
            public void setRackName(final String rackName)
            {
                myRackName = rackName;
            }
        }
    }

    /**
     * Configuration for host-aware connections.
     */
    public static final class HostAware
    {
        private Map<String, Host> myHosts = new HashMap<>();

        /**
         * Default constructor for HostAware.
         */
        public HostAware()
        {

        }

        /**
         * Gets the hosts map.
         *
         * @return the hosts map.
         */
        @JsonProperty("hosts")
        public Map<String, Host> getHosts()
        {
            return myHosts;
        }

        /**
         * Sets the hosts.
         *
         * @param hosts
         *         a list of hosts.
         */
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

    /**
     * Represents a host configuration with a hostname and port.
     */
    public static final class Host
    {
        private static final int DEFAULT_PORT = 9042;
        private String myHost = "localhost";

        private int myPort = DEFAULT_PORT;

        /**
         * Default constructor for Host. Initializes the host with default values.
         */
        public Host()
        {

        }

        /**
         * Constructs a Host with the specified hostname and port.
         *
         * @param host
         *         the hostname.
         * @param port
         *         the port number.
         */
        public Host(final String host, final int port)
        {
            myHost = host;
            myPort = port;
        }

        /**
         * Gets the hostname of the host.
         *
         * @return the hostname.
         */
        @JsonProperty("host")
        public String getHost()
        {
            return myHost;
        }

        /**
         * Gets the port number of the host.
         *
         * @return the port number.
         */
        @JsonProperty("port")
        public int getPort()
        {
            return myPort;
        }

        /**
         * Sets the hostname of the host.
         *
         * @param host
         *         the hostname to set.
         */
        @JsonProperty("host")
        public void setHost(final String host)
        {
            myHost = host;
        }

        /**
         * Sets the port number of the host.
         *
         * @param port
         *         the port number to set.
         */
        @JsonProperty("port")
        public void setPort(final int port)
        {
            myPort = port;
        }
    }

    public static class Timeout
    {
        private long myTime = 0;
        private TimeUnit myTimeUnit = TimeUnit.MILLISECONDS;

        public final long getConnectionTimeout(final TimeUnit timeUnit)
        {
            return timeUnit.convert(myTime, myTimeUnit);
        }

        @JsonProperty("time")
        public final void setTime(final long time)
        {
            myTime = time;
        }

        @JsonProperty("unit")
        public final void setTimeUnit(final String timeUnit)
        {
            myTimeUnit = TimeUnit.valueOf(timeUnit.toUpperCase(Locale.US));
        }
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
                        IpTranslator.class,
                };
    }
}
