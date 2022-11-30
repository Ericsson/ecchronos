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
package com.ericsson.bss.cassandra.ecchronos.application.config; // NOPMD

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultJmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.DefaultNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.application.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.application.NoopStatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.application.ReloadingCertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.DefaultRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;
import com.ericsson.bss.cassandra.ecchronos.fm.impl.LoggingFaultReporter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.ApplicationContext;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;

@SuppressWarnings({"checkstyle:methodname", "checkstyle:membername"})
public class Config
{
    private ConnectionConfig connection = new ConnectionConfig();
    private GlobalRepairConfig repair = new GlobalRepairConfig();
    private StatisticsConfig statistics = new StatisticsConfig();
    private LockFactoryConfig lock_factory = new LockFactoryConfig();
    private RunPolicyConfig run_policy = new RunPolicyConfig();
    private SchedulerConfig scheduler = new SchedulerConfig();
    private RestServerConfig rest_server = new RestServerConfig();

    public final ConnectionConfig getConnectionConfig()
    {
        return connection;
    }

    public final void setConnection(final ConnectionConfig aConnection)
    {
        if (aConnection != null)
        {
            this.connection = aConnection;
        }
    }

    /**
     * Get the global repair configuration.
     *
     * @return GlobalRepairConfig
     */
    public GlobalRepairConfig getRepair()
    {
        return repair;
    }

    /**
     * Set repair configuration.
     *
     * @param repairConfig The repair configuration.
     */
    public void setRepair(final GlobalRepairConfig repairConfig)
    {
        if (repairConfig != null)
        {
            this.repair = repairConfig;
        }
    }

    public final StatisticsConfig getStatistics()
    {
        return statistics;
    }

    public final void setStatistics(final StatisticsConfig theStatistics)
    {
        if (theStatistics != null)
        {
            this.statistics = theStatistics;
        }
    }

    public final LockFactoryConfig getLockFactory()
    {
        return lock_factory;
    }

    public final void setLock_factory(final LockFactoryConfig lockFactory)
    {
        if (lockFactory != null)
        {
            this.lock_factory = lockFactory;
        }
    }

    public final RunPolicyConfig getRunPolicy()
    {
        return run_policy;
    }

    public final void setRun_policy(final RunPolicyConfig runPolicy)
    {
        if (runPolicy != null)
        {
            this.run_policy = runPolicy;
        }
    }

    public final SchedulerConfig getScheduler()
    {
        return scheduler;
    }

    /**
     * Set the scheduler.
     *
     * @param aScheduler The scheduler.
     */
    public void setScheduler(final SchedulerConfig aScheduler)
    {
        if (aScheduler != null)
        {
            this.scheduler = aScheduler;
        }
    }

    public final RestServerConfig getRestServer()
    {
        return rest_server;
    }

    public final void setRest_server(final RestServerConfig restServer)
    {
        if (restServer != null)
        {
            this.rest_server = restServer;
        }
    }

    public static class ConnectionConfig
    {
        private NativeConnection cql = new NativeConnection();
        private JmxConnection jmx = new JmxConnection();

        public final NativeConnection getCql()
        {
            return cql;
        }

        public final JmxConnection getJmx()
        {
            return jmx;
        }

        public final void setCql(final NativeConnection aCQL)
        {
            if (aCQL != null)
            {
                this.cql = aCQL;
            }
        }

        public final void setJmx(final JmxConnection aJMX)
        {
            if (aJMX != null)
            {
                this.jmx = aJMX;
            }
        }

        @Override
        public final String toString()
        {
            return String.format("Connection(native=%s, jmx=%s)", cql, jmx);
        }
    }

    public abstract static class Connection<T>
    {
        private String host = "localhost";
        //protected int port;
        private int port;
        //protected Class<? extends T> provider;
        //protected Class<? extends CertificateHandler> certificateHandler;
        private Class<? extends T> provider;
        private Class<? extends CertificateHandler> certificateHandler;
        private Timeout timeout = new Timeout();

        public final String getHost()
        {
            return host;
        }

        public final int getPort()
        {
            return port;
        }

        public final Class<? extends T> getProviderClass()
        {
            return provider;
        }

        public final Class<? extends CertificateHandler> getCertificateHandlerClass()
        {
            return certificateHandler;
        }

        public final Timeout getTimeout()
        {
            return timeout;
        }

        public final void setHost(final String aHost)
        {
            this.host = aHost;
        }

        public final void setPort(final int aPort)
        {
            this.port = aPort;
        }

        public final void setTimeout(final Timeout aTimeout)
        {
            this.timeout = aTimeout;
        }

        public final void setProvider(final Class<? extends T> aProvider) throws NoSuchMethodException
        {
            aProvider.getDeclaredConstructor(expectedConstructor());

            this.provider = aProvider;
        }

        protected abstract Class<?>[] expectedConstructor();

        /**
         * Set certification handler.
         *
         * @param aCertificateHandler The certification handler.
         * @throws NoSuchMethodException If the getDeclaredConstructor method was not found.
         */
        public void setCertificateHandler(final Class<? extends CertificateHandler> aCertificateHandler)
                throws NoSuchMethodException
        {
            aCertificateHandler.getDeclaredConstructor(expectedCertHandlerConstructor());

            this.certificateHandler = aCertificateHandler;
        }

        /**
         * Get the expected certification handler.
         *
         * @return CertificateHandler
         */
        protected Class<?>[] expectedCertHandlerConstructor()
        {
            return new Class<?>[]
                    {
                            Supplier.class
                    };
        }

        /**
         * String representation.
         *
         * @return String
         */
        @Override
        public String toString()
        {
            return String.format("(%s:%d:%s),provider=%s,certificateHandler=%s",
                    host, port, timeout, provider, certificateHandler);
        }

        public static class Timeout
        {
            private long time = 0;
            private TimeUnit unit = TimeUnit.MILLISECONDS;

            public final long getConnectionTimeout(final TimeUnit aUnit)
            {
                return aUnit.convert(time, this.unit);
            }

            public final void setTime(final long aTime)
            {
                this.time = aTime;
            }

            public final void setUnit(final String aUnit)
            {
                this.unit = TimeUnit.valueOf(aUnit.toUpperCase(Locale.US));
            }
        }
    }

    public static class NativeConnection extends Connection<NativeConnectionProvider>
    {
        private static final int DEFAULT_PORT = 9042;

        private Class<? extends StatementDecorator> decoratorClass = NoopStatementDecorator.class;
        private boolean remoteRouting = true;

        public NativeConnection()
        {
            try
            {
                this.setProvider(DefaultNativeConnectionProvider.class);
                this.setCertificateHandler(ReloadingCertificateHandler.class);
                this.setPort(DEFAULT_PORT);
            }
            catch (NoSuchMethodException ignored)
            {
                // Do something useful ...
            }
        }

        public final Class<? extends StatementDecorator> getDecoratorClass()
        {
            return decoratorClass;
        }

        public final void setDecoratorClass(final Class<StatementDecorator> aDecoratorClass)
                throws NoSuchMethodException
        {
            aDecoratorClass.getDeclaredConstructor(Config.class);

            this.decoratorClass = aDecoratorClass;
        }

        public final boolean getRemoteRouting()
        {
            return remoteRouting;
        }

        public final void setRemoteRouting(final boolean aRemoteRouting)
        {
            this.remoteRouting = aRemoteRouting;
        }

        @Override
        protected final Class<?>[] expectedConstructor()
        {
            return new Class<?>[]
                    {
                            Config.class, Supplier.class, DefaultRepairConfigurationProvider.class, MeterRegistry.class
                    };
        }

        @Override
        public final String toString()
        {
            return String.format("(%s:%d),provider=%s,certificateHandler=%s,decorator=%s",
                    getHost(), getPort(), getProviderClass(), getCertificateHandlerClass(), decoratorClass);
        }
    }

    public static class JmxConnection extends Connection<JmxConnectionProvider>
    {
        private static final int DEFAULT_PORT = 7199;

        public JmxConnection()
        {
            try
            {
                this.setProvider(DefaultJmxConnectionProvider.class);
                this.setPort(DEFAULT_PORT);
            }
            catch (NoSuchMethodException ignored)
            {
                // Do something useful ...
            }
        }

        @Override
        protected final Class<?>[] expectedConstructor()
        {
            return new Class<?>[]
                    {
                            Config.class, Supplier.class
                    };
        }
    }

    public static class GlobalRepairConfig extends RepairConfig
    {
        private static final int THIRTY_DAYS = 30;

        private Class<? extends AbstractRepairConfigurationProvider> provider = FileBasedRepairConfiguration.class;
        private RepairLockType lockType = RepairLockType.VNODE;
        private Interval historyLookback = new Interval(THIRTY_DAYS, TimeUnit.DAYS);
        private RepairHistory history = new RepairHistory();

        public final RepairLockType getLockType()
        {
            return lockType;
        }

        public final Interval getHistoryLookback()
        {
            return historyLookback;
        }

        public final void setLock_type(final String aLockType)
        {
            this.lockType = RepairLockType.valueOf(aLockType.toUpperCase(Locale.US));
        }

        public final void setHistory_lookback(final Interval aHistoryLookback)
        {
            this.historyLookback = aHistoryLookback;
        }

        public final RepairHistory getHistory()
        {
            return history;
        }

        public final void setHistory(final RepairHistory aHistory)
        {
            this.history = aHistory;
        }

        public final Class<? extends AbstractRepairConfigurationProvider> getProvider()
        {
            return provider;
        }

        public final void setProvider(final Class<? extends AbstractRepairConfigurationProvider> aProvider)
                throws NoSuchMethodException
        {
            provider.getDeclaredConstructor(ApplicationContext.class);

            this.provider = aProvider;
        }
    }

    public static class RepairHistory
    {
        public enum Provider
        {
            CASSANDRA, UPGRADE, ECC
        }

        private Provider provider = Provider.ECC;
        private String keyspace = "ecchronos";

        public final Provider getProvider()
        {
            return provider;
        }

        public final void setProvider(final String aProvider)
        {
            this.provider = Provider.valueOf(aProvider.toUpperCase(Locale.US));
        }

        public final String getKeyspace()
        {
            return keyspace;
        }

        public final void setKeyspace(final String aKeyspace)
        {
            this.keyspace = aKeyspace;
        }
    }

    public static class Alarm
    {
        private static final int DEFAULT_EIGHT_DAYS = 8;
        private static final int DEFAULT_TEN_DAYS = 10;

        private Class<? extends RepairFaultReporter> faultReporter = LoggingFaultReporter.class;
        private Interval warn = new Interval(DEFAULT_EIGHT_DAYS, TimeUnit.DAYS);
        private Interval error = new Interval(DEFAULT_TEN_DAYS, TimeUnit.DAYS);

        public Alarm()
        {
            // Default constructor for jackson
        }

        public Alarm(final Interval warningInterval, final Interval errorInterval)
        {
            this.warn = warningInterval;
            this.error = errorInterval;
        }

        public final Class<? extends RepairFaultReporter> getFaultReporter()
        {
            return faultReporter;
        }

        public final Interval getWarn()
        {
            return warn;
        }

        public final Interval getError()
        {
            return error;
        }

        public final void setFaultReporter(final Class<? extends RepairFaultReporter> aFaultReporter)
        {
            this.faultReporter = aFaultReporter;
        }

        public final void setWarn(final Interval warningInterval)
        {
            this.warn = warningInterval;
        }

        public final void setError(final Interval errorInterval)
        {
            this.error = errorInterval;
        }
    }

    public static class StatisticsConfig
    {
        private boolean enabled = true;
        private File directory = new File("./statistics");
        private ReportingConfigs reporting = new ReportingConfigs();
        private String prefix = "";

        public final boolean isEnabled()
        {
            boolean isAnyReportingEnabled = reporting.isFileReportingEnabled()
                    || reporting.isJmxReportingEnabled()
                    || reporting.isHttpReportingEnabled();
            return enabled && isAnyReportingEnabled;
        }

        public final File getDirectory()
        {
            return directory;
        }

        public final ReportingConfigs getReporting()
        {
            return reporting;
        }

        public final String getPrefix()
        {
            return prefix;
        }

        public final void setEnabled(final boolean enabledValue)
        {
            this.enabled = enabledValue;
        }

        public final void setDirectory(final String aDirectory)
        {
            this.directory = new File(aDirectory);
        }

        public final void setReporting(final ReportingConfigs theReportings)
        {
            this.reporting = theReportings;
        }

        public final void setPrefix(final String aPrefix)
        {
            this.prefix = aPrefix;
        }
    }

    public static class ReportingConfigs
    {
        private ReportingConfig jmx = new ReportingConfig();
        private ReportingConfig file = new ReportingConfig();
        private ReportingConfig http = new ReportingConfig();

        public final ReportingConfig getJmx()
        {
            return jmx;
        }

        public final ReportingConfig getFile()
        {
            return file;
        }

        public final ReportingConfig getHttp()
        {
            return http;
        }

        public final void setJmx(final ReportingConfig jmxConfig)
        {
            this.jmx = jmxConfig;
        }

        public final void setFile(final ReportingConfig fileConfig)
        {
            this.file = fileConfig;
        }

        public final void setHttp(final ReportingConfig httpConfig)
        {
            this.http = httpConfig;
        }

        public final boolean isHttpReportingEnabled()
        {
            return http != null && http.isEnabled();
        }

        public final boolean isJmxReportingEnabled()
        {
            return jmx != null && jmx.isEnabled();
        }

        public final boolean isFileReportingEnabled()
        {
            return file != null && file.isEnabled();
        }
    }

    public static class ReportingConfig
    {
        private boolean enabled = true;
        private Set<ExcludedMetric> excludedMetrics = new HashSet<>();

        public final boolean isEnabled()
        {
            return enabled;
        }

        public final void setEnabled(final boolean enabledValue)
        {
            this.enabled = enabledValue;
        }

        public final Set<ExcludedMetric> getExcludedMetrics()
        {
            return excludedMetrics;
        }

        public final void setExcludedMetrics(final Set<ExcludedMetric> theExcludedMetrics)
        {
            this.excludedMetrics = theExcludedMetrics;
        }
    }

    public static class ExcludedMetric
    {
        private String name;
        private Map<String, String> tags = new HashMap<>();

        public final String getName()
        {
            return name;
        }

        public final void setName(final String aName)
        {
            this.name = aName;
        }

        public final Map<String, String> getTags()
        {
            return tags;
        }

        public final void setTags(final Map<String, String> theTags)
        {
            this.tags = theTags;
        }

        @Override
        public final boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            ExcludedMetric that = (ExcludedMetric) o;
            return Objects.equals(name, that.name) && Objects.equals(tags, that.tags);
        }

        @Override
        public final int hashCode()
        {
            return Objects.hash(name, tags);
        }
    }

    public static class LockFactoryConfig
    {
        private CasLockFactoryConfig cas = new CasLockFactoryConfig();

        public final CasLockFactoryConfig getCas()
        {
            return cas;
        }

        public final void setCas(final CasLockFactoryConfig aCas)
        {
            this.cas = aCas;
        }
    }

    public static class CasLockFactoryConfig
    {
        private String keyspace = "ecchronos";

        public final String getKeyspace()
        {
            return keyspace;
        }

        public final void setKeyspace(final String aKeyspace)
        {
            this.keyspace = aKeyspace;
        }
    }

    public static class RunPolicyConfig
    {
        private TimeBasedConfig timeBased = new TimeBasedConfig();

        public final TimeBasedConfig getTimeBased()
        {
            return timeBased;
        }

        public final void setTime_based(final TimeBasedConfig timeBasedValue)
        {
            this.timeBased = timeBasedValue;
        }
    }

    public static class TimeBasedConfig
    {
        private String keyspace = "ecchronos";

        public final String getKeyspace()
        {
            return keyspace;
        }

        public final void setKeyspace(final String aKeyspace)
        {
            this.keyspace = aKeyspace;
        }
    }

    public static class SchedulerConfig
    {
        private static final int THIRTY_SECONDS = 30;

        private Interval frequency = new Interval(THIRTY_SECONDS, TimeUnit.SECONDS);

        public final Interval getFrequency()
        {
            return frequency;
        }

        public final void setFrequency(final Interval aFrequency)
        {
            this.frequency = aFrequency;
        }
    }

    public static class RestServerConfig
    {
        private static final String DEFAULT_HOST = "localhost";
        private static final int DEFAULT_PORT = 8080;

        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;

        public final String getHost()
        {
            return host;
        }

        public final void setHost(final String aHost)
        {
            this.host = aHost;
        }

        public final int getPort()
        {
            return port;
        }

        public final void setPort(final int aPort)
        {
            this.port = aPort;
        }
    }

    public static class Interval
    {
        private long time;
        private TimeUnit unit;

        public Interval()
        {
            // Default constructor for jackson
        }

        public Interval(final long aTime, final TimeUnit aUnit)
        {
            this.time = aTime;
            this.unit = aUnit;
        }

        public final long getInterval(final TimeUnit aUnit)
        {
            return aUnit.convert(time, this.unit);
        }

        public final void setTime(final long aTime)
        {
            this.time = aTime;
        }

        public final void setUnit(final String aUnit)
        {
            this.unit = TimeUnit.valueOf(aUnit.toUpperCase(Locale.US));
        }
    }
}
