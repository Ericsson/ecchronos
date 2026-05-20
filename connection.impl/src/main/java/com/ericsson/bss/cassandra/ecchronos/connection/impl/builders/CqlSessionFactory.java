/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public final class CqlSessionFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(CqlSessionFactory.class);

    private static final List<String> SCHEMA_REFRESHED_KEYSPACES = ImmutableList.of("/.*/", "!system",
            "!system_distributed", "!system_schema", "!system_traces", "!system_views", "!system_virtual_schema");

    private static final List<String> SESSION_METRICS = Arrays.asList(DefaultSessionMetric.BYTES_RECEIVED.getPath(),
            DefaultSessionMetric.BYTES_SENT.getPath(), DefaultSessionMetric.CONNECTED_NODES.getPath(),
            DefaultSessionMetric.CQL_REQUESTS.getPath(), DefaultSessionMetric.CQL_CLIENT_TIMEOUTS.getPath(),
            DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_DELAY.getPath(),
            DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(), DefaultSessionMetric.THROTTLING_ERRORS.getPath());

    private CqlSessionFactory()
    {
    }

    /**
     * Create a CqlSession with the given configuration.
     */
    public static CqlSession create(final List<InetSocketAddress> contactPoints,
                                    final String localDatacenter,
                                    final AuthProvider authProvider,
                                    final SslEngineFactory sslEngineFactory,
                                    final SchemaChangeListener schemaChangeListener,
                                    final Set<NodeStateListener> nodeStateListeners,
                                    final boolean metricsEnabled)
    {
        CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .addContactPoints(resolveIfNeeded(contactPoints))
                .withLocalDatacenter(localDatacenter)
                .withAuthProvider(authProvider)
                .withSslEngineFactory(sslEngineFactory)
                .withSchemaChangeListener(schemaChangeListener);

        for (NodeStateListener listener : nodeStateListeners)
        {
            sessionBuilder = sessionBuilder.addNodeStateListener(listener);
        }

        DriverConfigLoader configLoader = buildDriverConfig(metricsEnabled).build();
        LOG.debug("Driver configuration: {}", configLoader.getInitialConfig().getDefaultProfile().entrySet());
        sessionBuilder.withConfigLoader(configLoader);

        return sessionBuilder.build();
    }

    private static ProgrammaticDriverConfigLoaderBuilder buildDriverConfig(final boolean metricsEnabled)
    {
        ProgrammaticDriverConfigLoaderBuilder loaderBuilder = DriverConfigLoader.programmaticBuilder()
                .withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, SCHEMA_REFRESHED_KEYSPACES);
        if (metricsEnabled)
        {
            loaderBuilder.withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED, SESSION_METRICS);
            loaderBuilder.withString(DefaultDriverOption.METRICS_FACTORY_CLASS, "MicrometerMetricsFactory");
            loaderBuilder.withString(DefaultDriverOption.METRICS_ID_GENERATOR_CLASS, "TaggingMetricIdGenerator");
        }
        return loaderBuilder;
    }

    private static Collection<InetSocketAddress> resolveIfNeeded(final List<InetSocketAddress> contactPoints)
    {
        List<InetSocketAddress> resolved = new ArrayList<>();
        for (InetSocketAddress address : contactPoints)
        {
            if (address.isUnresolved())
            {
                resolved.add(new InetSocketAddress(address.getHostString(), address.getPort()));
            }
            else
            {
                resolved.add(address);
            }
        }
        return resolved;
    }
}
