/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.data.iptranslator;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public final class IpTranslator extends NodeStateListenerBase
{
    private static final Logger LOG = LoggerFactory.getLogger(IpTranslator.class);

    private static final String KEYSPACE_NAME = "system_views";
    private static final String TABLE_NAME = "gossip_info";
    private static final String COLUMN_EXTERNAL_ADDRESS = "address";
    private static final String COLUMN_INTERNAL_ADDRESS = "internal_address_and_port";

    private CqlSession mySession;
    private BoundStatement mySelectStatement;
    private final Map<String, String> myIpMap = new ConcurrentHashMap<>();

    public IpTranslator()
    {
        // Do nothing
    }

    public void init(final CqlSession session)
    {
        if (mySession != null)
        {
            return;
        }

        mySession = Preconditions.checkNotNull(session, "Session cannot be null");

        mySelectStatement = mySession.prepare(selectFrom(KEYSPACE_NAME, TABLE_NAME)
            .columns(COLUMN_EXTERNAL_ADDRESS, COLUMN_INTERNAL_ADDRESS)
            .build()
            .setConsistencyLevel(ConsistencyLevel.ONE)).bind();

        refreshIpMap();
    }

    public boolean isActive()
    {
        return !myIpMap.isEmpty();
    }

    public String getInternalIp(final String externalIp)
    {
        String internalIp = myIpMap.get(externalIp);
        if (internalIp  == null)
        {
            LOG.warn("No internal IP found for external IP: {}", externalIp);
            return externalIp;
        }
        return internalIp;
    }

    private void refreshIpMap()
    {
        if (mySession == null || mySelectStatement == null)
        {
            return;
        }

        ResultSet result = mySession.execute(mySelectStatement);
        result.forEach(row ->
        {
            String externalIP = row.getInetAddress(COLUMN_EXTERNAL_ADDRESS).getHostAddress();
            String internalIP = row.getString(COLUMN_INTERNAL_ADDRESS);
            myIpMap.put(externalIP, internalIP);
        });
    }

    @Override
    public void onAdd(final Node node)
    {
        refreshIpMap();
    }

    @Override
    public void onUp(final Node node)
    {
        refreshIpMap();
    }
}
