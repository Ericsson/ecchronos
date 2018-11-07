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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.DefaultRepairConfigurationProviderComponent;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.TableStorageStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.ScheduleManagerService;
import com.ericsson.bss.cassandra.ecchronos.core.osgi.TimeBasedRunPolicyService;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import org.apache.felix.scr.Component;
import org.apache.felix.scr.ScrService;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.osgi.framework.BundleContext;
import org.osgi.util.tracker.ServiceTracker;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.awaitility.Awaitility.await;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class ITTestECChronos extends TestBase
{
    private static final String SCHEDULE_MANAGER_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.ScheduleManagerService";

    private static final String TIME_BASED_RUN_POLICY_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.TimeBasedRunPolicyService";
    private static final String REPAIR_CONFIGURATION_PID = "com.ericsson.bss.cassandra.ecchronos.core.osgi.DefaultRepairConfigurationProviderComponent";

    @Inject
    BundleContext myBundleContext;

    @Inject
    ScrService myScrService;

    @Configuration
    public Option[] configure() throws IOException
    {
        return basicOptions();
    }

    @Before
    public void verifyConfiguration() throws InterruptedException
    {
        checkNativeConnection();
        checkJmxConnection();
    }

    @Test
    public void testGetLockFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, LockFactory.class, null);
        serviceTracker.open();

        LockFactory lockFactory = (LockFactory) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(lockFactory);
    }

    @Test
    public void testGetHostStatesService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, HostStates.class, null);
        serviceTracker.open();

        HostStates hostStates = (HostStates) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(hostStates);
    }

    @Test
    public void testGetJmxProxyFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, JmxProxyFactory.class, null);
        serviceTracker.open();
        JmxProxyFactory jmxProxyFactory = (JmxProxyFactory) serviceTracker.waitForService(10000);
        serviceTracker.close();

        assertNotNull(jmxProxyFactory);
    }

    @Test
    public void testGetReplicatedTableProviderService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, ReplicatedTableProvider.class, null);
        serviceTracker.open();
        ReplicatedTableProvider replicatedTableProvider = (ReplicatedTableProvider) serviceTracker.waitForService(10000);
        serviceTracker.close();

        assertNotNull(replicatedTableProvider);
    }

    @Test
    public void testGetTableStorageStatesService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, TableStorageStates.class, null);
        serviceTracker.open();

        TableStorageStates tableStorageStates = (TableStorageStates) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(tableStorageStates);
    }

    @Test
    public void testGetTableRepairMetricsService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, TableRepairMetrics.class, null);
        serviceTracker.open();

        TableRepairMetrics tableRepairMetrics = (TableRepairMetrics) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(tableRepairMetrics);
    }

    @Test
    public void testGetRepairStateFactoryService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, RepairStateFactory.class, null);
        serviceTracker.open();

        RepairStateFactory repairStateFactory = (RepairStateFactory) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(repairStateFactory);
    }

    @Test
    public void testGetRepairSchedulerService() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, RepairScheduler.class, null);
        serviceTracker.open();

        RepairScheduler repairScheduler = (RepairScheduler) serviceTracker.waitForService(TimeUnit.SECONDS.toMillis(10));
        serviceTracker.close();

        assertNotNull(repairScheduler);
    }

    @Test
    public void testGetScheduleManagerComponent()
    {
        assertComponentIsActive(SCHEDULE_MANAGER_PID, ScheduleManagerService.class);
    }

    @Test
    public void testGetTimeBasedRunPolicyComponent()
    {
        assertComponentIsActive(TIME_BASED_RUN_POLICY_PID, TimeBasedRunPolicyService.class);
    }

    @Test
    public void testGetRepairConfigurationComponent()
    {
        assertComponentIsActive(REPAIR_CONFIGURATION_PID, DefaultRepairConfigurationProviderComponent.class);
    }

    private void assertComponentIsActive(String componentPid, Class clazz)
    {
        await().atMost(10, TimeUnit.SECONDS).until(() -> componentIsActive(componentPid, clazz));

        assertEquals(Component.STATE_ACTIVE, getComponent(componentPid, clazz).getState());
    }

    private boolean componentIsActive(String componentPid, Class clazz)
    {
        return getComponent(componentPid, clazz).getState() == Component.STATE_ACTIVE;
    }

    private Component getComponent(String componentPid, Class clazz)
    {
        Component[] components = myScrService.getComponents(componentPid);

        assertNotNull(components);
        assertEquals(1, components.length);

        Component component = components[0];
        assertEquals(clazz.getCanonicalName(), component.getClassName());

        return component;
    }

    private void checkNativeConnection() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, NativeConnectionProvider.class, null);
        serviceTracker.open();

        NativeConnectionProvider nativeConnectionProvider = (NativeConnectionProvider) serviceTracker.waitForService(10000);
        if (nativeConnectionProvider == null)
        {
            throw new IllegalStateException("Native Connection provider not started");
        }
        serviceTracker.close();
    }

    private void checkJmxConnection() throws InterruptedException
    {
        ServiceTracker serviceTracker = new ServiceTracker(myBundleContext, JmxConnectionProvider.class, null);
        serviceTracker.open();
        JmxConnectionProvider jmxConnectionProvider = (JmxConnectionProvider) serviceTracker.waitForService(10000);
        if (jmxConnectionProvider == null)
        {
            throw new IllegalStateException("JMX Connection provider not started");
        }
        serviceTracker.close();
    }
}
