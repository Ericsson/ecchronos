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

import com.ericsson.bss.cassandra.ecchronos.core.CASLockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

import java.util.Map;

@Component(service = LockFactory.class)
@Designate(ocd = CASLockFactoryService.Configuration.class)
public class CASLockFactoryService implements LockFactory
{
    private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";

    @Reference(service = NativeConnectionProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile NativeConnectionProvider myNativeConnectionProvider;

    @Reference (service = StatementDecorator.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile StatementDecorator myStatementDecorator;

    @Reference (service = HostStates.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile HostStates myHostStates;

    private volatile CASLockFactory myDelegateLockFactory;

    @Activate
    public final synchronized void activate(final Configuration configuration)
    {
        myDelegateLockFactory = CASLockFactory.builder()
                .withNativeConnectionProvider(myNativeConnectionProvider)
                .withHostStates(myHostStates)
                .withStatementDecorator(myStatementDecorator)
                .withKeyspaceName(configuration.keyspaceName())
                .build();
    }

    @Deactivate
    public final synchronized void deactivate()
    {
        myDelegateLockFactory.close();
    }

    @Override
    public final DistributedLock tryLock(final String dataCenter,
                                         final String resource,
                                         final int priority,
                                         final Map<String, String> metadata)
            throws LockException
    {
        return myDelegateLockFactory.tryLock(dataCenter, resource, priority, metadata);
    }

    @Override
    public final Map<String, String> getLockMetadata(final String dataCenter, final String resource)
    {
        return myDelegateLockFactory.getLockMetadata(dataCenter, resource);
    }

    @Override
    public final boolean sufficientNodesForLocking(final String dataCenter, final String resource)
    {
        return myDelegateLockFactory.sufficientNodesForLocking(dataCenter, resource);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "The lock factory keyspace to use",
                description = "The name of the keyspace containing the lock factory tables")
        String keyspaceName() default DEFAULT_KEYSPACE_NAME;
    }
}
