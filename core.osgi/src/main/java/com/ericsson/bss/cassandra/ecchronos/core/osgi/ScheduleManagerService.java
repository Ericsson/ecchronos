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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManagerImpl;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

@Component(service = ScheduleManager.class)
@Designate(ocd = ScheduleManagerService.Configuration.class)
public class ScheduleManagerService implements ScheduleManager
{
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleManagerService.class);

    private static final long DEFAULT_SCHEDULE_INTERVAL_IN_SECONDS = 60L;

    @Reference(service = RunPolicy.class,
            cardinality = ReferenceCardinality.MULTIPLE,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindRunPolicy",
            unbind = "unbindRunPolicy")
    private final Set<RunPolicy> myRunPolicies = Sets.newConcurrentHashSet();

    @Override
    public final String getCurrentJobStatus()
    {
        return myDelegateSchedulerManager.getCurrentJobStatus();
    }

    @Reference (service = LockFactory.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.STATIC)
    private volatile LockFactory myLockFactory;

    private volatile ScheduleManagerImpl myDelegateSchedulerManager;

    @Activate
    public final synchronized void activate(final Configuration configuration)
    {
        long scheduleIntervalInSeconds = configuration.scheduleIntervalInSeconds();

        myDelegateSchedulerManager = ScheduleManagerImpl.builder()
                .withLockFactory(myLockFactory)
                .withRunInterval(scheduleIntervalInSeconds, TimeUnit.SECONDS)
                .build();

        for (RunPolicy runPolicy : myRunPolicies)
        {
            myDelegateSchedulerManager.addRunPolicy(runPolicy);
        }
    }

    @Deactivate
    public final synchronized void deactivate()
    {
        myDelegateSchedulerManager.close();
    }

    @Override
    public final void schedule(final ScheduledJob job)
    {
        myDelegateSchedulerManager.schedule(job);
    }

    @Override
    public final void deschedule(final ScheduledJob job)
    {
        myDelegateSchedulerManager.deschedule(job);
    }

    public final synchronized void bindRunPolicy(final RunPolicy runPolicy)
    {
        if (myRunPolicies.add(runPolicy))
        {
            LOG.debug("Run policy {} added", runPolicy);
            if (myDelegateSchedulerManager != null)
            {
                myDelegateSchedulerManager.addRunPolicy(runPolicy);
            }
        }
        else
        {
            LOG.warn("Run policy {} already added", runPolicy);
        }
    }

    public final synchronized void unbindRunPolicy(final RunPolicy runPolicy)
    {
        if (myRunPolicies.remove(runPolicy))
        {
            LOG.debug("Run policy {} removed", runPolicy);
            if (myDelegateSchedulerManager != null)
            {
                myDelegateSchedulerManager.removeRunPolicy(runPolicy);
            }
        }
        else
        {
            LOG.warn("Run policy {} already removed", runPolicy);
        }
    }

    @ObjectClassDefinition
    public @interface Configuration
    {
        @AttributeDefinition(name = "Schedule interval in seconds",
                description = "The interval in which jobs will be scheduled to run")
        long scheduleIntervalInSeconds() default DEFAULT_SCHEDULE_INTERVAL_IN_SECONDS;
    }
}
