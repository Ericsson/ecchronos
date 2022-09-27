/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.osgi;

import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.cm.ConfigurationAdminOptions;
import org.ops4j.pax.exam.cm.ConfigurationOption;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.MavenUrlReference;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import static org.ops4j.pax.exam.CoreOptions.*;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.configureConsole;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.editConfigurationFilePut;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.features;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;

public class TestBase
{
    protected static final String CASSANDRA_HOST_PROPERTY = "it-cassandra.ip";
    protected static final String CASSANDRA_NATIVE_PORT_PROPERTY = "it-cassandra.native.port";
    protected static final String CASSANDRA_JMX_PORT_PROPERTY = "it-cassandra.jmx.port";

    protected static final String CASSANDRA_HOST = System.getProperty(CASSANDRA_HOST_PROPERTY);
    protected static final String CASSANDRA_NATIVE_PORT = System.getProperty(CASSANDRA_NATIVE_PORT_PROPERTY);
    protected static final String CASSANDRA_JMX_PORT = System.getProperty(CASSANDRA_JMX_PORT_PROPERTY);
    protected static final String IS_LOCAL = System.getProperty("it-local-cassandra");

    protected static final String REPAIR_METRICS_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.TableRepairMetricsService";

    protected static final String NATIVE_CONNECTION_PID = "com.ericsson.bss.cassandra.ecchronos.connection.impl.OSGiLocalNativeConnectionProvider";
    protected static final String JMX_CONNECTION_PID = "com.ericsson.bss.cassandra.ecchronos.connection.impl.OSGiLocalJmxConnectionProvider";

    protected static final String CONFIGURATION_NATIVE_HOST = "localHost";
    protected static final String CONFIGURATION_NATIVE_PORT = "nativePort";
    protected static final String CONFIGURATION_JMX_HOST = "jmxHost";
    protected static final String CONFIGURATION_JMX_PORT = "jmxPort";

    protected static final String CONFIGURATION_CREDENTIALS_FILE = "credentialsFile";

    protected static final String CONFIGURATION_STATISTICS_DIRECTORY = "metricsDirectory";

    private static final File topDirectory = new File("target", "exam");

    public Option[] basicOptions() throws IOException
    {
        MavenUrlReference karafArtifactUrl = maven()
                .groupId("org.apache.karaf")
                .artifactId("apache-karaf")
                .versionAsInProject()
                .type("zip");

        MavenUrlReference karafStandardUrl = maven()
                .groupId("org.apache.karaf.features")
                .artifactId("standard")
                .versionAsInProject()
                .classifier("features")
                .type("xml");

        MavenUrlReference ecChronosKarafFeaturesMvnUrl = maven()
                .groupId("com.ericsson.bss.cassandra.ecchronos")
                .artifactId("karaf-feature")
                .type("xml")
                .classifier("features")
                .versionAsInProject();

        return options(
                karafDistributionConfiguration()
                        .frameworkUrl(karafArtifactUrl)
                        .unpackDirectory(topDirectory)
                        .useDeployFolder(false),
                logLevel(LogLevelOption.LogLevel.INFO),
                features(karafStandardUrl, "scr", "standard"),
                editConfigurationFilePut("etc/org.apache.karaf.shell.cfg", "sshHost", "localhost"),
                editConfigurationFilePut("etc/org.apache.karaf.shell.cfg", "sshPort", "8888"),
                keepRuntimeFolder(),
                configureConsole().ignoreLocalConsole(),
                configureConsole().startRemoteShell(),
                features(ecChronosKarafFeaturesMvnUrl, "karaf-feature"),
                mavenBundle("com.ericsson.bss.cassandra.ecchronos", "connection.impl").versionAsInProject(),
                mavenBundle("com.ericsson.bss.cassandra.ecchronos", "fm.impl").versionAsInProject(),
                mavenBundle("org.assertj", "assertj-core").versionAsInProject(),
                mavenBundle("org.awaitility", "awaitility").versionAsInProject(),
                nativeConnectionOptions(),
                jmxConnectionOptions(),
                metricsOptions(),
                junitBundles(),
                propagateSystemProperties(CASSANDRA_HOST_PROPERTY, CASSANDRA_NATIVE_PORT_PROPERTY)
        );
    }

    protected Option nativeConnectionOptions()
    {
        ConfigurationOption configurationOption = ConfigurationAdminOptions.newConfiguration(NATIVE_CONNECTION_PID)
                .put(CONFIGURATION_NATIVE_HOST, CASSANDRA_HOST)
                .put(CONFIGURATION_NATIVE_PORT, CASSANDRA_NATIVE_PORT);

        if (IS_LOCAL == null)
        {
            URL url = TestBase.class.getClassLoader().getResource("credentials.properties");

            if (url != null)
            {
                configurationOption = configurationOption.put(CONFIGURATION_CREDENTIALS_FILE, url.getPath());
            }
        }

        return configurationOption.asOption();
    }

    protected Option jmxConnectionOptions()
    {
        return ConfigurationAdminOptions.newConfiguration(JMX_CONNECTION_PID)
                .put(CONFIGURATION_JMX_HOST, CASSANDRA_HOST)
                .put(CONFIGURATION_JMX_PORT, CASSANDRA_JMX_PORT)
                .asOption();
    }

    protected Option metricsOptions() throws IOException
    {
        File metricsDirectory = new File(topDirectory, "metrics").getAbsoluteFile();

        if (!metricsDirectory.exists())
        {
            Files.createDirectories(metricsDirectory.toPath());
        }

        return ConfigurationAdminOptions.newConfiguration(REPAIR_METRICS_PID)
                .put(CONFIGURATION_STATISTICS_DIRECTORY, metricsDirectory.getAbsolutePath())
                .asOption();
    }
}
