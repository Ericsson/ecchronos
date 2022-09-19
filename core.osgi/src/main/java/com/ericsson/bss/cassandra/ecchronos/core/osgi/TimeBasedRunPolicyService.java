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

import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@Component(service = {RunPolicy.class, TableRepairPolicy.class})
@Designate(ocd = TimeBasedRunPolicyService.Configuration.class)
public class TimeBasedRunPolicyService implements RunPolicy, TableRepairPolicy
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

    private volatile TimeBasedRunPolicy myDelegatePolicy;

    @Activate
    public final synchronized void activate(final Configuration configuration)
    {
        myDelegatePolicy = TimeBasedRunPolicy.builder()
                .withSession(myNativeConnectionProvider.getSession())
                .withStatementDecorator(myStatementDecorator)
                .withKeyspaceName(configuration.keyspaceName())
                .build();
    }

    @Deactivate
    public final synchronized void deactivate()
    {
        myDelegatePolicy.close();
    }

    @Override
    public final long validate(final ScheduledJob job)
    {
        return myDelegatePolicy.validate(job);
    }

    @Override
    public final boolean shouldRun(final TableReference tableReference)
    {
        return myDelegatePolicy.shouldRun(tableReference);
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "The time based runpolicy keyspace to use",
                description = "The name of the keyspace containing the time based runpolicy tables")
        String keyspaceName() default DEFAULT_KEYSPACE_NAME;
    }
}
