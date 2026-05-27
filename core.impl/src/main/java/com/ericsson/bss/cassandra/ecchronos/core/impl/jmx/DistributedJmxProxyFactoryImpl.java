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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;

import com.ericsson.bss.cassandra.ecchronos.data.iptranslator.IpTranslator;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;

import java.util.Map;
import java.util.UUID;
import javax.management.MalformedObjectNameException;
import java.io.IOException;


/**
 * A factory creating JMX proxies to Cassandra.
 */
public final class  DistributedJmxProxyFactoryImpl implements DistributedJmxProxyFactory
{
    private final DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
    private final Map<UUID, Node> nodesMap;
    private final EccNodesSync eccNodesSync;
    private final boolean isJolokiaEnabled;
    private final Integer myMaxWaitTimeInMinutes;
    private final JolokiaNotificationController myJolokiaNotificationController;

    private DistributedJmxProxyFactoryImpl(final Builder builder)
    {
        myDistributedJmxConnectionProvider = builder.myDistributedJmxConnectionProvider;
        nodesMap = builder.myNodesMap;
        eccNodesSync = builder.myEccNodesSync;
        isJolokiaEnabled = builder.isJolokiaEnabled;
        myMaxWaitTimeInMinutes = builder.myMaxWaitTimeInMinutes;
        myJolokiaNotificationController = builder.myJolokiaController;
    }

    @Override
    public DistributedJmxProxy connect() throws IOException
    {
        try
        {
            if (isJolokiaEnabled)
            {
                return new JolokiaJmxProxy(
                        myDistributedJmxConnectionProvider,
                        nodesMap,
                        eccNodesSync,
                        myJolokiaNotificationController);
            }
            else
            {
                return new RMIJmxProxy(
                        myDistributedJmxConnectionProvider,
                        nodesMap,
                        eccNodesSync);
            }
        }
        catch (MalformedObjectNameException e)
        {
            throw new IOException("Unable to get StorageService object", e);
        }
    }
    @Override
    public Integer getMaxWaitTimeInMinutes()
    {
        return myMaxWaitTimeInMinutes;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        public static final int DEFAULT_RUN_DELAY = 500;
        public static final int DEFAULT_MAX_WAIT_TIME_IN_MINUTES = 40;
        private DistributedJmxConnectionProvider myDistributedJmxConnectionProvider;
        private Map<UUID, Node> myNodesMap;
        private EccNodesSync myEccNodesSync;
        private boolean isJolokiaEnabled = false;
        private Integer myMaxWaitTimeInMinutes = DEFAULT_MAX_WAIT_TIME_IN_MINUTES;
        private IpTranslator myIpTranslator;
        private JolokiaNotificationController myJolokiaController;

        /**
         * Build with JMX connection provider.
         *
         * @param distributedJmxConnectionProvider The JMX connection provider
         * @return Builder
         */
        public Builder withJmxConnectionProvider(final DistributedJmxConnectionProvider distributedJmxConnectionProvider)
        {
            myDistributedJmxConnectionProvider = distributedJmxConnectionProvider;
            return this;
        }

        /**
         * Build with Nodes map.
         *
         * @param nodesMap The Nodes map
         * @return Builder
         */
        public Builder withNodesMap(final Map<UUID, Node> nodesMap)
        {
            myNodesMap = nodesMap;
            return this;
        }

        /**
         * Build with EccNodesSync.
         *
         * @param eccNodesSync The EccNodesSync
         * @return Builder
         */
        public Builder withEccNodesSync(final EccNodesSync eccNodesSync)
        {
            myEccNodesSync = eccNodesSync;
            return this;
        }

        /**
         * Build with Jolokia Agent.
         *
         * @param jolokiaEnabled Define if Jolokia Client must be used.
         * @return Builder
         */
        public Builder withJolokiaEnabled(final boolean jolokiaEnabled)
        {
            isJolokiaEnabled = jolokiaEnabled;
            return this;
        }

        /**
         * Build with maxWaitTimeInMinutes.
         *
         * @param maxWaitTimeInMinutes Integer
         * @return Builder
         */
        public Builder withMaxWaitTimeInMinutes(final Integer maxWaitTimeInMinutes)
        {
            myMaxWaitTimeInMinutes = maxWaitTimeInMinutes;
            return this;
        }

        /**
         * Build with IpTranslator.
         *
         * @param ipTranslator
         * @return Builder
         */
        public Builder withIpTranslator(final IpTranslator ipTranslator)
        {
            myIpTranslator = ipTranslator;
            return this;
        }

        /**
         * Build with JolokiaNotificationController.
         *
         * @param jolokiaNotificationController The Custom Notification Controller responsible for managing notifications in jolokia.
         * @return Builder
         */
        public Builder withJolokiaNotificationController(final JolokiaNotificationController jolokiaNotificationController)
        {
            myJolokiaController = jolokiaNotificationController;
            return this;
        }

        /**
         * Build.
         *
         * @return DistributedJmxProxyFactoryImpl
         */
        public DistributedJmxProxyFactoryImpl build()
        {
            if (myDistributedJmxConnectionProvider == null)
            {
                throw new IllegalArgumentException("JMX Connection provider cannot be null");
            }
            if (myIpTranslator == null)
            {
                throw new IllegalArgumentException("IpTranslator cannot be null");
            }
            return new DistributedJmxProxyFactoryImpl(this);
        }
    }
}
