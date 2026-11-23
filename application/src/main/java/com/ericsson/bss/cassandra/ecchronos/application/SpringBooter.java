/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application;

import com.ericsson.bss.cassandra.ecchronos.rest.ConfigManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.LoggingRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.MetricsRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.RejectConfigREST;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairSessionsRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.ScheduleRepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.StateManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.VersionRESTImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;

@SpringBootApplication
@Import(value = { RepairManagementRESTImpl.class, ScheduleRepairManagementRESTImpl.class,
        OnDemandRepairManagementRESTImpl.class, StateManagementRESTImpl.class, MetricsRESTImpl.class,
        RejectConfigREST.class, ConfigManagementRESTImpl.class, RepairSessionsRESTImpl.class,
        VersionRESTImpl.class, LoggingRESTImpl.class})
public class SpringBooter extends SpringBootServletInitializer
{
    private static final Logger LOG = LoggerFactory.getLogger(SpringBooter.class);

    private final VersionRESTImpl myVersionREST;

    public SpringBooter(final VersionRESTImpl versionREST)
    {
        myVersionREST = versionREST;
    }

    /**
     * Logs the ecChronos agent version once the application context is fully started.
     *
     * @param event the application ready event
     */
    @EventListener(ApplicationReadyEvent.class)
    public final void logVersionOnStartup(final ApplicationReadyEvent event)
    {
        LOG.info("Started ecChronos agent version {} (commit {})",
                myVersionREST.getVersionString(), myVersionREST.getCommit());
    }

    public static void main(final String[] args)
    {
        // Route embedded Tomcat's java.util.logging through SLF4J/logback so its output shares
        // ecChronos' formatting and is controlled by logback.xml. Remove the default JUL handlers
        // first to avoid duplicate console output, then install the bridge. Must run before Spring
        // starts the embedded web server so the connector/TLS startup logs are captured.
        // Works together with logback's LevelChangePropagator, which keeps the JUL level in sync so
        // suppressed levels are dropped at the source rather than bridged and filtered.
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        try
        {
            SpringApplication.run(SpringBooter.class, args);
        }
        catch (Exception e)
        {
            LOG.error("Failed to initialize", e);
            System.exit(1);
        }
    }
}
