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
package com.ericsson.bss.cassandra.ecchronos.core.osgi;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.HostStatesImpl;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import java.net.InetAddress;

/**
 * Implementation of the {@link HostStates} interface using JMX to retrieve node statuses and then caches the retrieved
 * statuses for some time.
 */
@Component(service = HostStates.class)
public class HostStatesService implements HostStates
{
    @Reference(service = JmxProxyFactory.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile JmxProxyFactory myJmxProxyFactory;

    private volatile HostStatesImpl myDelegateHostStates;

    @Activate
    public final void activate()
    {
        myDelegateHostStates = HostStatesImpl.builder()
                .withJmxProxyFactory(myJmxProxyFactory)
                .build();
    }

    @Deactivate
    public final void deactivate()
    {
        myDelegateHostStates.close();
    }

    @Override
    public final boolean isUp(final InetAddress address)
    {
        return myDelegateHostStates.isUp(address);
    }

    @Override
    public final boolean isUp(final Node node)
    {
        return myDelegateHostStates.isUp(node);
    }

    @Override
    public final boolean isUp(final DriverNode node)
    {
        return myDelegateHostStates.isUp(node);
    }
}
