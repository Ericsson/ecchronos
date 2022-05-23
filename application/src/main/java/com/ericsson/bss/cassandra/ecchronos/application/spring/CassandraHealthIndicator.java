/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

package com.ericsson.bss.cassandra.ecchronos.application.spring;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.ericsson.bss.cassandra.ecchronos.connection.JmxConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.JmxProxyFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

@Component
public class CassandraHealthIndicator implements HealthIndicator
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraHealthIndicator.class);
    private final NativeConnectionProvider myNativeConnectionProvider;
    private final JmxConnectionProvider myJmxConnectionProvider;

    public CassandraHealthIndicator(NativeConnectionProvider nativeConnectionProvider,
            JmxConnectionProvider jmxConnectionProvider)
    {
        myNativeConnectionProvider = nativeConnectionProvider;
        myJmxConnectionProvider = jmxConnectionProvider;
    }

    @Override
    public Health health()
    {
        Map<String, Object> details = new HashMap<>();
        boolean cqlUp = isCqlConnectionUp(details);
        boolean jmxUp = isJmxConnectionUp(details);
        if (cqlUp && jmxUp)
        {
            return Health.up().withDetails(details).build();
        }
        return Health.down().withDetails(details).build();
    }

    private boolean isJmxConnectionUp(Map<String, Object> details)
    {
        try
        {
            JmxProxyFactory jmxProxyFactory = JmxProxyFactoryImpl.builder()
                    .withJmxConnectionProvider(myJmxConnectionProvider)
                    .build();
            JmxProxy jmxProxy = jmxProxyFactory.connect();
            jmxProxy.close();
            return true;
        }
        catch (Exception e)
        {
            LOG.debug("JMX readiness failed", e);
            LOG.error("JMX readiness failed {}", e.getMessage());
            details.put("JMX connection error:", e.getMessage());
        }
        return false;
    }

    private boolean isCqlConnectionUp(Map<String, Object> details)
    {
        try
        {
            BuiltStatement selectFromLocal = select().from("system", "local");
            Session session = myNativeConnectionProvider.getSession();
            ResultSet result = session.execute(selectFromLocal);
            if (!result.all().isEmpty())
            {
                return true;
            }
        }
        catch (Exception e)
        {
            LOG.debug("CQL readiness failed", e);
            LOG.error("CQL readiness failed {}", e.getMessage());
            details.put("CQL connection error:", e.getMessage());
        }
        return false;
    }
}
