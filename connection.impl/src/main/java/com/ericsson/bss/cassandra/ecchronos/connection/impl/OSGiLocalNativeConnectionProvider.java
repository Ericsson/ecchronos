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
package com.ericsson.bss.cassandra.ecchronos.connection.impl;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Component(service = NativeConnectionProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class OSGiLocalNativeConnectionProvider implements NativeConnectionProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(OSGiLocalNativeConnectionProvider.class);

    private static final String USERNAME_KEY = "username";
    private static final String PASSWORD_KEY = "password";
    private static final String DEFAULT_USERNAME = "cassandra";
    private static final String DEFAULT_PASSWORD = "cassandra";

    private volatile LocalNativeConnectionProvider myDelegateNativeConnectionProvider;

    @Activate
    public synchronized void activate(Configuration configuration)
    {
        String localhost = configuration.localHost();
        int port = configuration.nativePort();
        boolean remoteRouting = configuration.remoteRouting();

        LocalNativeConnectionProvider.Builder builder = LocalNativeConnectionProvider.builder()
                .withLocalhost(localhost)
                .withPort(port)
                .withRemoteRouting(remoteRouting);

        if (!configuration.credentialsFile().isEmpty())
        {
            try (InputStream inputStream = new FileInputStream(configuration.credentialsFile()))
            {
                Properties properties = new Properties();
                properties.load(inputStream);
                String username = properties.getProperty(USERNAME_KEY, DEFAULT_USERNAME);
                String password = properties.getProperty(PASSWORD_KEY, DEFAULT_PASSWORD);
                builder.withAuthProvider(new PlainTextAuthProvider(username, password));
            }
            catch (IOException e)
            {
                LOG.error("Unable to read provided credentials file {}, will not be using authentication", configuration.credentialsFile());
            }
        }

        myDelegateNativeConnectionProvider = builder.build();
    }

    @Deactivate
    public synchronized void deactivate()
    {
        myDelegateNativeConnectionProvider.close();
    }

    @Override
    public Session getSession()
    {
        return myDelegateNativeConnectionProvider.getSession();
    }

    @Override
    public Host getLocalHost()
    {
        return myDelegateNativeConnectionProvider.getLocalHost();
    }

    @Override
    public boolean getRemoteRouting()
    {
        return myDelegateNativeConnectionProvider.getRemoteRouting();
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Cassandra native port", description = "The port to use for native communication with Cassandra")
        int nativePort() default LocalNativeConnectionProvider.DEFAULT_NATIVE_PORT;

        @AttributeDefinition(name = "Cassandra host", description = "The host to use for native communication with Cassandra")
        String localHost() default LocalNativeConnectionProvider.DEFAULT_LOCAL_HOST;

        @AttributeDefinition(name = "Credentials file", description = "A file containing credentials for communication with Cassandra")
        String credentialsFile() default "";

        @AttributeDefinition(name = "Remote routing", description = "Enables remote routing between datacenters")
        boolean remoteRouting() default true;
    }
}
