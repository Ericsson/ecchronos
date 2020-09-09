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

import java.io.IOException;

import javax.management.remote.JMXConnector;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(service = JmxConnectionProvider.class, configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = OSGiLocalJmxConnectionProvider.Configuration.class)
public class OSGiLocalJmxConnectionProvider implements JmxConnectionProvider
{
    private volatile LocalJmxConnectionProvider myDelegateJmxConnectionProvider;

    @Activate
    public synchronized void activate(Configuration configuration) throws IOException
    {
        String localhost = configuration.jmxHost();
        int port = configuration.jmxPort();

        myDelegateJmxConnectionProvider = new LocalJmxConnectionProvider(localhost, port);
    }

    @Deactivate
    public synchronized void deactivate() throws IOException
    {
        myDelegateJmxConnectionProvider.close();
    }

    @Override
    public JMXConnector getJmxConnector() throws IOException
    {
        return myDelegateJmxConnectionProvider.getJmxConnector();
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Cassandra JMX Port", description = "The port to use for JMX communication with Cassandra")
        int jmxPort() default LocalJmxConnectionProvider.DEFAULT_PORT;

        @AttributeDefinition(name = "Cassandra JMX Host", description = "The host to use for JMX communication with Cassandra")
        String jmxHost() default LocalJmxConnectionProvider.DEFAULT_HOST;
    }
}
