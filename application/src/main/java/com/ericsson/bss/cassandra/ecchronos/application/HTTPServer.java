/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.io.Closeable;
import java.net.InetSocketAddress;

public class HTTPServer implements Closeable
{
    private final Server myServer;

    public HTTPServer(RepairScheduler repairScheduler, ScheduleManager scheduleManager, InetSocketAddress inetSocketAddress)
    {
        ResourceConfig config = new ResourceConfig()
                .packages(true, RepairManagementRESTImpl.class.getPackage().getName())
                .register(new MyBinder(repairScheduler, scheduleManager));

        ServletHolder servletHolder = new ServletHolder(new ServletContainer(config));

        servletHolder.setInitOrder(0);
        servletHolder.setInitParameter(ServerProperties.PROVIDER_CLASSNAMES, RepairManagementRESTImpl.class.getCanonicalName());

        myServer = new Server(inetSocketAddress);
        ServletContextHandler context = new ServletContextHandler(myServer, "/");
        context.addServlet(servletHolder, "/*");
    }

    public void start() throws HTTPServerException
    {
        try
        {
            myServer.start();
            myServer.join();
        }
        catch (Exception e)
        {
            throw new HTTPServerException("Unable to start HTTP server", e);
        }
    }

    @Override
    public void close()
    {
        myServer.destroy();
    }

    private class MyBinder extends AbstractBinder
    {
        private final RepairScheduler myRepairScheduler;
        private final ScheduleManager myScheduleManager;

        public MyBinder(RepairScheduler repairScheduler, ScheduleManager scheduleManager)
        {
            myRepairScheduler = repairScheduler;
            myScheduleManager = scheduleManager;
        }

        @Override
        protected void configure()
        {
            bind(myRepairScheduler).to(RepairScheduler.class);
            bind(myScheduleManager).to(ScheduleManager.class);
        }
    }
}
