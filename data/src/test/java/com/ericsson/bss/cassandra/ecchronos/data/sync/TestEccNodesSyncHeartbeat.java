/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.data.sync;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.data.utils.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the self-healing behaviour for issue #1826: a heartbeat write applies a TTL to the
 * entire row, so the row survives while it is renewed and disappears completely once renewal
 * stops (i.e. once the owning instance is gone).
 */
@NotThreadSafe
public class TestEccNodesSyncHeartbeat extends AbstractCassandraTest
{
    private static final String KEYSPACE = "ecchronos";
    private static final String TABLE = "nodes_sync";
    private static final String ECC_ID = "heartbeat-test";
    private static final String DC = "datacenter1";

    private EccNodesSync eccNodesSync;
    private final DistributedNativeConnectionProvider nativeConnectionProvider = getNativeConnectionProvider();

    @Before
    public void setup() throws IOException
    {
        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                        + "{'class': 'NetworkTopologyStrategy', 'DC1': 1}", KEYSPACE));
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s("
                        + "ecchronos_id TEXT, datacenter_name TEXT, node_id UUID, node_endpoint TEXT, "
                        + "node_status TEXT, last_connection TIMESTAMP, next_connection TIMESTAMP, "
                        + "PRIMARY KEY(ecchronos_id, datacenter_name, node_id)) "
                        + "WITH CLUSTERING ORDER BY(datacenter_name DESC, node_id DESC);", KEYSPACE, TABLE));

        eccNodesSync = EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(nativeConnectionProvider)
                .withConnectionDelayValue(Long.valueOf(10))
                .withConnectionDelayUnit(TimeUnit.MINUTES)
                .withEcchronosID(ECC_ID)
                .build();
    }

    @After
    public void cleanup()
    {
        mySession.execute(SimpleStatement.newInstance(
                String.format("TRUNCATE %s.%s", KEYSPACE, TABLE)));
    }

    @Test
    public void heartbeatAppliesTtlToAllColumnsAndWholeRowExpires()
    {
        UUID nodeId = UUID.randomUUID();
        int shortTtl = 3; // seconds

        // Write a heartbeat with a short TTL.
        eccNodesSync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC, "10.0.0.1", nodeId, shortTtl);

        // Immediately after: the row exists, AVAILABLE, and EVERY value column carries a TTL.
        List<Row> rows = fetchWithTtls(nodeId);
        assertThat(rows).hasSize(1);
        Row row = rows.get(0);
        assertThat(row.getString("node_status")).isEqualTo(NodeStatus.AVAILABLE.name());
        assertThat(row.isNull("status_ttl")).as("node_status must carry a TTL").isFalse();
        assertThat(row.isNull("endpoint_ttl")).as("node_endpoint must carry a TTL").isFalse();
        assertThat(row.isNull("last_conn_ttl")).as("last_connection must carry a TTL").isFalse();
        assertThat(row.isNull("next_conn_ttl")).as("next_connection must carry a TTL").isFalse();

        // After TTL elapses without renewal: the WHOLE row is gone (no ghost primary-key row).
        await().atMost(shortTtl + 10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> countRows(nodeId) == 0);

        assertThat(countRows(nodeId))
                .as("whole row must expire once heartbeat stops")
                .isEqualTo(0);
    }

    @Test
    public void repeatedHeartbeatsKeepRowAlive()
    {
        UUID nodeId = UUID.randomUUID();
        int shortTtl = 3;

        // Renew several times within the TTL window; the row must stay alive throughout.
        for (int i = 0; i < 4; i++)
        {
            eccNodesSync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC, "10.0.0.1", nodeId, shortTtl);
            assertThat(countRows(nodeId)).as("row alive while being renewed").isEqualTo(1);
            sleep(1);
        }

        // Stop renewing -> row expires.
        await().atMost(shortTtl + 10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> countRows(nodeId) == 0);
        assertThat(countRows(nodeId)).isEqualTo(0);
    }

    private long countRows(final UUID nodeId)
    {
        ResultSet rs = mySession.execute(SimpleStatement.newInstance(
                String.format("SELECT node_id FROM %s.%s WHERE ecchronos_id = ? AND datacenter_name = ? "
                        + "AND node_id = ?", KEYSPACE, TABLE), ECC_ID, DC, nodeId));
        List<Row> rows = new ArrayList<>();
        rs.forEach(rows::add);
        return rows.size();
    }

    private List<Row> fetchWithTtls(final UUID nodeId)
    {
        ResultSet rs = mySession.execute(SimpleStatement.newInstance(
                String.format("SELECT node_status, "
                        + "TTL(node_status) AS status_ttl, "
                        + "TTL(node_endpoint) AS endpoint_ttl, "
                        + "TTL(last_connection) AS last_conn_ttl, "
                        + "TTL(next_connection) AS next_conn_ttl "
                        + "FROM %s.%s WHERE ecchronos_id = ? AND datacenter_name = ? AND node_id = ?",
                        KEYSPACE, TABLE), ECC_ID, DC, nodeId));
        List<Row> rows = new ArrayList<>();
        rs.forEach(rows::add);
        return rows;
    }

    private void sleep(final int seconds)
    {
        try
        {
            TimeUnit.SECONDS.sleep(seconds);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
