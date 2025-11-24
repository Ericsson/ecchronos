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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.oss.driver.api.core.CqlSession;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.repair.Interval;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.ReloadingCertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.metadata.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.data.repairhistory.RepairHistoryService;
import com.ericsson.bss.cassandra.ecchronos.data.sync.EccNodesSync;

import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import java.net.InetAddress;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigRefresher;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.TLSConfig;
import com.ericsson.bss.cassandra.ecchronos.application.providers.AgentNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.DefaultRepairConfigurationProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.boot.convert.ApplicationConversionService;

/**
 * The {@code BeanConfigurator} class is responsible for configuring and managing beans within a Spring application
 * context, particularly related to Cassandra connections and security configurations. It also provides support for
 * refreshing security settings dynamically based on configuration file changes.
 */
@Configuration
public class BeanConfigurator
{
    private static final Logger LOG = LoggerFactory.getLogger(BeanConfigurator.class);

    private static final String CONFIGURATION_FILE = "ecc.yml";
    private static final String SECURITY_FILE = "security.yml";
    private static final String ECCHORONS_ID_PRE_STRING = "ecchronos-";

    private final AtomicReference<Security.CqlSecurity> cqlSecurity = new AtomicReference<>();
    private final AtomicReference<Security.JmxSecurity> jmxSecurity = new AtomicReference<>();
    private final ConfigRefresher configRefresher;
    private final String ecChronosID;

    /**
     * Constructs a new {@code BeanConfigurator} and initializes the configuration and security settings. If the
     * application is configured to use a specific path, the configuration refresher is initialized to watch for changes
     * in security settings.
     *
     * @throws ConfigurationException
     *         if there is an error loading the configuration files.
     * @throws UnknownHostException
     *         if the local host name cannot be determined.
     */
    public BeanConfigurator() throws ConfigurationException, UnknownHostException
    {
        if (ConfigurationHelper.DEFAULT_INSTANCE.usePath())
        {
            configRefresher = new ConfigRefresher(ConfigurationHelper.DEFAULT_INSTANCE.getConfigPath());
            configRefresher.watch(ConfigurationHelper.DEFAULT_INSTANCE.configFile(SECURITY_FILE).toPath(),
                    () -> refreshSecurityConfig(cqlSecurity::set, jmxSecurity::set));
        }
        else
        {
            configRefresher = null;
        }
        Security security = getSecurityConfig();
        cqlSecurity.set(security.getCqlSecurity());
        jmxSecurity.set(security.getJmxSecurity());
        ecChronosID = getConfiguration().getConnectionConfig().getCqlConnection().getInstanceName();
    }

    /**
     * Closes the {@code ConfigRefresher} and releases any resources held by it.
     */
    public final void close()
    {
        if (configRefresher != null)
        {
            configRefresher.close();
        }
    }

    /**
     * Provides a {@link Config} bean that represents the application configuration.
     *
     * @return the {@link Config} object.
     * @throws ConfigurationException
     *         if there is an error loading the configuration.
     */
    @Bean
    public Config config() throws ConfigurationException
    {
        return getConfiguration();
    }

    /**
     * Provides a {@link WebMvcConfigurer} bean to configure formatters and converters for the Spring MVC framework.
     *
     * @return a {@link WebMvcConfigurer} object.
     */
    @Bean
    public WebMvcConfigurer conversionConfigurer() //Add application converters to web so springboot can convert in REST
    {
        return new WebMvcConfigurer()
        {
            @Override
            public void addFormatters(final FormatterRegistry registry)
            {
                ApplicationConversionService.configure(registry);
            }
        };
    }

    /**
     * Provides a {@link DefaultRepairConfigurationProvider} bean.
     *
     * @return a {@link DefaultRepairConfigurationProvider} object.
     */
    @Bean
    public DefaultRepairConfigurationProvider defaultRepairConfigurationProvider()
    {
        return new DefaultRepairConfigurationProvider();
    }

    /**
     * Configures the embedded web server factory with the host and port specified in the application configuration.
     *
     * @param config
     *         the {@link Config} object containing the server configuration.
     * @return a configured {@link ConfigurableServletWebServerFactory}.
     * @throws UnknownHostException
     *         if the specified host cannot be resolved.
     */
    @Bean
    public ConfigurableServletWebServerFactory webServerFactory(final Config config) throws UnknownHostException
    {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setAddress(InetAddress.getByName(config.getRestServer().getHost()));
        factory.setPort(config.getRestServer().getPort());
        return factory;
    }

    @Bean
    public RepairFaultReporter repairFaultReporter(final Config config) throws ConfigurationException
    {
        return ReflectionUtils.construct(config.getRepairConfig().getAlarm().getFaultReporterClass());
    }

    /**
     * Provides a {@link DistributedNativeConnectionProvider} bean to manage Cassandra native connections.
     *
     * @param config
     *         the {@link Config} object containing the Cassandra connection configuration.
     * @return a {@link DistributedNativeConnectionProvider} instance.
     */
    @Bean
    public DistributedNativeConnectionProvider distributedNativeConnectionProvider(
            final Config config,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider
    )
    {
        return getDistributedNativeConnection(config, cqlSecurity::get, defaultRepairConfigurationProvider);
    }

    /**
     * Provides an {@link EccNodesSync} bean for synchronizing nodes in an ecChronos environment.
     *
     * @param distributedNativeConnectionProvider
     *         the provider for Cassandra native connections.
     * @return an {@link EccNodesSync} instance.
     * @throws UnknownHostException
     *         if the local host name cannot be determined.
     * @throws EcChronosException
     *         if there is an error during node synchronization.
     * @throws ConfigurationException
     *         if there is an error during node synchronization.
     */
    @Bean
    public EccNodesSync eccNodesSync(
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider
    ) throws UnknownHostException, EcChronosException, ConfigurationException
    {
        return getEccNodesSync(distributedNativeConnectionProvider);
    }

    /**
     * Provides a {@link DistributedJmxConnectionProvider} bean for managing JMX connections to Cassandra nodes.
     *
     * @param distributedNativeConnectionProvider
     *         the provider for Cassandra native connections.
     * @param eccNodesSync
     *         the {@link EccNodesSync} instance for node synchronization.
     * @return a {@link DistributedJmxConnectionProvider} instance.
     * @throws IOException
     *         if there is an error creating the JMX connection provider.
     */
    @Bean
    public DistributedJmxConnectionProvider distributedJmxConnectionProvider(
            final Config config,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final EccNodesSync eccNodesSync
    ) throws IOException
    {
        return getDistributedJmxConnection(
                config, jmxSecurity::get, distributedNativeConnectionProvider, eccNodesSync);
    }

    @Bean
    public RetrySchedulerService retrySchedulerService(final Config config,
                                                       final DistributedJmxConnectionProvider jmxConnectionProvider,
                                                       final EccNodesSync eccNodesSync,
                                                       final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        return new RetrySchedulerService(eccNodesSync, config, jmxConnectionProvider, nativeConnectionProvider);
    }

    @Bean
    public ReloadSchedulerService reloadSchedulerService(
        final Config config,
        final DistributedNativeConnectionProvider nativeConnectionProvider,
        final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider)
    {
        return new ReloadSchedulerService(config, nativeConnectionProvider, defaultRepairConfigurationProvider);
    }

    @Bean
    public NodeResolver nodeResolver(final DistributedNativeConnectionProvider distributedNativeConnectionProvider)
    {
        CqlSession session = distributedNativeConnectionProvider.getCqlSession();
        return new NodeResolverImpl(session);
    }

    @Bean
    public ReplicationState replicationState(
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final NodeResolver nodeResolver)
    {
        CqlSession session = distributedNativeConnectionProvider.getCqlSession();
        return new ReplicationStateImpl(nodeResolver, session);
    }

    @Bean
    public RepairHistoryService repairHistoryService(
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final NodeResolver nodeResolver,
            final ReplicationState replicationState,
            final Config config
    )
    {
        return getRepairHistoryService(distributedNativeConnectionProvider, nodeResolver, replicationState, config);
    }

    private Security getSecurityConfig() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(SECURITY_FILE, Security.class);
    }

    private Config getConfiguration() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(CONFIGURATION_FILE, Config.class);
    }

    private DistributedNativeConnectionProvider getDistributedNativeConnection(
            final Config config,
            final Supplier<Security.CqlSecurity> securitySupplier,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider
    )
    {
        Supplier<TLSConfig> tlsSupplier = () -> securitySupplier.get().getCqlTlsConfig();
        CertificateHandler certificateHandler = createCertificateHandler(tlsSupplier);
        return new AgentNativeConnectionProvider(
                config,
                securitySupplier,
                certificateHandler,
                defaultRepairConfigurationProvider);
    }

    private DistributedJmxConnectionProvider getDistributedJmxConnection(
            final Config config,
            final Supplier<Security.JmxSecurity> securitySupplier,
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final EccNodesSync eccNodesSync
    ) throws IOException
    {
        Supplier<TLSConfig> jmxTlsSupplier = () -> securitySupplier.get().getJmxTlsConfig();
        CertificateHandler certificateHandler = null;
        if (jmxTlsSupplier.get().isEnabled() && jmxTlsSupplier.get().isCertificateConfigured())
        {
            LOG.info("Creating Certificate handler for JMX with PEM certificates");
            certificateHandler = createCertificateHandler(jmxTlsSupplier);
        }
        return new AgentJmxConnectionProvider(
                config, securitySupplier, distributedNativeConnectionProvider, eccNodesSync, certificateHandler);
    }

    private void refreshSecurityConfig(
            final Consumer<Security.CqlSecurity> cqlSetter,
            final Consumer<Security.JmxSecurity> jmxSetter
    )
    {
        try
        {
            Security security = getSecurityConfig();
            cqlSetter.accept(security.getCqlSecurity());
            jmxSetter.accept(security.getJmxSecurity());
        }
        catch (ConfigurationException e)
        {
            LOG.warn("Unable to refresh security config");
        }
    }

    private static CertificateHandler createCertificateHandler(
            final Supplier<TLSConfig> tlsSupplier
    )
    {
        return new ReloadingCertificateHandler(tlsSupplier);
    }

    private EccNodesSync getEccNodesSync(
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider
    ) throws UnknownHostException, EcChronosException, ConfigurationException
    {
        Interval connectionDelay = config().getConnectionConfig().getCqlConnection().getConnectionDelay();
        EccNodesSync myEccNodesSync = EccNodesSync.newBuilder()
                .withNativeConnection(distributedNativeConnectionProvider)
                .withSession(distributedNativeConnectionProvider.getCqlSession())
                .withEcchronosID(ecChronosID)
                .withConnectionDelayValue(connectionDelay.getTime())
                .withConnectionDelayUnit(connectionDelay.getUnit())
                .build();
        myEccNodesSync.acquireNodes();
        LOG.info("Nodes acquired with success");
        return myEccNodesSync;
    }

    private RepairHistoryService getRepairHistoryService(
            final DistributedNativeConnectionProvider distributedNativeConnectionProvider,
            final NodeResolver nodeResolver,
            final ReplicationState replicationState,
            final Config config
    )
    {
        long interval = config.getRepairConfig().getRepairHistoryLookback().getInterval(TimeUnit.MILLISECONDS);
        long repairHistoryLookBack = TimeUnit.MILLISECONDS.convert(interval, config.getRepairConfig().getRepairHistoryLookback().getUnit());
        return new RepairHistoryService(
                distributedNativeConnectionProvider.getCqlSession(),
                replicationState,
                nodeResolver,
                repairHistoryLookBack
        );
    }
}
