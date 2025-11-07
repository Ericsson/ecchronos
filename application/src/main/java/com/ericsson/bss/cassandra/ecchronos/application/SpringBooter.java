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

import com.ericsson.bss.cassandra.ecchronos.rest.MetricsRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.OnDemandRepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.RepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.ScheduleRepairManagementRESTImpl;
import com.ericsson.bss.cassandra.ecchronos.rest.StateManagementRESTImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(value = { RepairManagementRESTImpl.class, ScheduleRepairManagementRESTImpl.class,
        OnDemandRepairManagementRESTImpl.class, StateManagementRESTImpl.class, MetricsRESTImpl.class})
public class SpringBooter extends SpringBootServletInitializer
{
    private static final Logger LOG = LoggerFactory.getLogger(SpringBooter.class);

    public static void main(final String[] args)
    {
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
