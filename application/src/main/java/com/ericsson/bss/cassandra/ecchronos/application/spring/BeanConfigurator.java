/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ericsson.bss.cassandra.ecchronos.application.ConfigurationException;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.ReflectionUtils;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigRefresher;
import com.ericsson.bss.cassandra.ecchronos.application.config.ConfigurationHelper;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.Security;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.ReplicationStateImpl;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolverImpl;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

import org.springframework.format.FormatterRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class BeanConfigurator
{
    private static final Logger LOG = LoggerFactory.getLogger(BeanConfigurator.class);

    private static final String CONFIGURATION_FILE = "ecc.yml";
    private static final String SECURITY_FILE = "security.yml";

    private final AtomicReference<Security.CqlSecurity> cqlSecurity = new AtomicReference<>();
    private final AtomicReference<Security.JmxSecurity> jmxSecurity = new AtomicReference<>();

    private final ConfigRefresher configRefresher;

    public BeanConfigurator() throws ConfigurationException
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
    }

    public final void close()
    {
        if (configRefresher != null)
        {
            configRefresher.close();
        }
    }

    @Bean
    public Config config() throws ConfigurationException
    {
        return getConfiguration();
    }

    private Security getSecurityConfig() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(SECURITY_FILE, Security.class);
    }

    private Config getConfiguration() throws ConfigurationException
    {
        return ConfigurationHelper.DEFAULT_INSTANCE.getConfiguration(CONFIGURATION_FILE, Config.class);
    }

    @Bean
    public ConfigurableServletWebServerFactory webServerFactory(final Config configuration) throws UnknownHostException
    {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.setAddress(InetAddress.getByName(configuration.getRestServer().getHost()));
        factory.setPort(configuration.getRestServer().getPort());
        return factory;
    }

    @Bean
    public RepairFaultReporter repairFaultReporter(final Config config) throws ConfigurationException
    {
        return ReflectionUtils.construct(config.getRepairConfig().getAlarm().getFaultReporterClass());
    }

    @Bean
    public DefaultRepairConfigurationProvider defaultRepairConfigurationProvider()
    {
        return new DefaultRepairConfigurationProvider();
    }

    @Bean
    public NativeConnectionProvider nativeConnectionProvider(final Config config,
            final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
            final MeterRegistry eccCompositeMeterRegistry) throws ConfigurationException
    {
        return getNativeConnectionProvider(config, cqlSecurity::get, defaultRepairConfigurationProvider,
                eccCompositeMeterRegistry);
    }

    public static NativeConnectionProvider getNativeConnectionProvider(
        final Config configuration,
        final Supplier<Security.CqlSecurity> securitySupplier,
        final DefaultRepairConfigurationProvider defaultRepairConfigurationProvider,
        final MeterRegistry meterRegistry) throws ConfigurationException
    {

        Supplier tlsSupplier = () -> securitySupplier.get().getCqlTlsConfig();
        CertificateHandler certificateHandler = createCertificateHandler(configuration, tlsSupplier);

        Class<?> providerClass = configuration.getConnectionConfig().getCqlConnection().getProviderClass();

        try
        {
            Object[] constructorArgs;
            if (DefaultNativeConnectionProvider.class.equals(providerClass))
            {
                constructorArgs = new Object[] {
                        configuration,
                        securitySupplier,
                        certificateHandler,
                        defaultRepairConfigurationProvider,
                        meterRegistry
                };
            }
            else
            {
                // Check for old versions of DefaultNativeConnectionProvider
                constructorArgs = new Object[]
                {
                        configuration,
                        securitySupplier,
                        defaultRepairConfigurationProvider,
                        meterRegistry
                };
            }

            return (NativeConnectionProvider) ReflectionUtils.construct(
                    providerClass,
                    getConstructorParameterTypes(providerClass),
                    constructorArgs);

        }
        catch (ConfigurationException ex)
        {
            throw new ConfigurationException(
                "Error while trying to connect with Cassandra using " + providerClass, ex);
        }
    }

    private static CertificateHandler createCertificateHandler(
        final Config configuration,
        final Supplier tlsSupplier) throws ConfigurationException
    {
        try
        {
            return ReflectionUtils.construct(
                    configuration.getConnectionConfig().getCqlConnection().getCertificateHandlerClass(),
                    new Class<?>[] {Supplier.class}, tlsSupplier);
        }
        catch (ConfigurationException e)
        {
            if (!(e.getCause() instanceof NoSuchMethodException))
            {
                throw e;
            }
            throw new ConfigurationException("""
                    Invalid configuration, connection provider does not support\
                     configured certificate handler.\
                    """, e);
        }
    }

    private static Class<?>[] getConstructorParameterTypes(final Class<?> providerClass)
    {
        if (DefaultNativeConnectionProvider.class.equals(providerClass))
        {
            return new Class<?>[] {
                    Config.class,
                    Supplier.class,
                    CertificateHandler.class,
                    DefaultRepairConfigurationProvider.class,
                    MeterRegistry.class
            };
        }
        else
        {
            return new Class<?>[]
            {
                    Config.class,
                    Supplier.class,
                    DefaultRepairConfigurationProvider.class,
                    MeterRegistry.class
            };
        }
    }

    @Bean
    public JmxConnectionProvider jmxConnectionProvider(final Config config) throws ConfigurationException
    {
        return getJmxConnectionProvider(config, jmxSecurity::get);
    }

    private static JmxConnectionProvider getJmxConnectionProvider(final Config configuration,
                                                                  final Supplier<Security.JmxSecurity> securitySupplier)
            throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getJmxConnection().getProviderClass(),
                        new Class<?>[] {
                                Config.class, Supplier.class
                        },
                        configuration, securitySupplier);
    }

    @Bean
    public StatementDecorator statementDecorator(final Config config) throws ConfigurationException
    {
        return getStatementDecorator(config);
    }

    private static StatementDecorator getStatementDecorator(final Config configuration) throws ConfigurationException
    {
        return ReflectionUtils
                .construct(configuration.getConnectionConfig().getCqlConnection().getDecoratorClass(), configuration);
    }

    private void refreshSecurityConfig(final Consumer<Security.CqlSecurity> cqlSetter,
                                       final Consumer<Security.JmxSecurity> jmxSetter)
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

    @Bean
    public NodeResolver nodeResolver(final NativeConnectionProvider nativeConnectionProvider)
    {
        CqlSession session = nativeConnectionProvider.getSession();

        return new NodeResolverImpl(session);
    }

    @Bean
    public ReplicationState replicationState(final NativeConnectionProvider nativeConnectionProvider,
                                             final NodeResolver nodeResolver)
    {
        Node node = nativeConnectionProvider.getLocalNode();
        CqlSession session = nativeConnectionProvider.getSession();

        return new ReplicationStateImpl(nodeResolver, session, node);
    }

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
}
