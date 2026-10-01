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
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.NodeSyncState;
import com.ericsson.bss.cassandra.ecchronos.data.utils.AbstractCassandraTest;
import com.ericsson.bss.cassandra.ecchronos.utils.enums.sync.NodeStatus;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the self-healing behaviour of the {@code nodes_sync} table against a real Cassandra
 * instance (Testcontainers) and the real {@link EccNodesSync} write path, using the exact
 * production schema (no {@code default_time_to_live}).
 *
 * <p>The table is partitioned per ecChronos instance ({@code ecchronos_id}), and each instance only
 * maintains its own partition. Without a self-healing mechanism, the rows of a permanently removed
 * instance (e.g. a decommissioned datacenter) are never cleaned up: nothing takes ownership of the
 * orphaned partition, and the table carries no TTL.</p>
 *
 * <p>The tests cover:</p>
 * <ul>
 *   <li>the legacy problem: orphaned rows remain {@code AVAILABLE} indefinitely with no TTL;</li>
 *   <li>the heartbeat + TTL fix: a renewed row survives, and the whole row expires once renewal
 *       stops;</li>
 *   <li>staleness derivation on the read path from {@code last_connection}.</li>
 * </ul>
 */
@NotThreadSafe
public class NodesSyncSelfHealingIT extends AbstractCassandraTest
{
    private static final String KEYSPACE = "ecchronos";
    private static final String TABLE = "nodes_sync";

    private static final String DC1_ID = "dc1-ecchronos";
    private static final String DC3_ID = "dc3-ecchronos";
    private static final String DC1_NAME = "dc1";
    private static final String DC3_NAME = "dc3";

    @Before
    public void setup()
    {
        mySession.execute(String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
                        + "{'class': 'NetworkTopologyStrategy', 'DC1': 1}",
                KEYSPACE));

        // Exact production schema from create_keyspace_sample.cql: NO default_time_to_live.
        mySession.execute(String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s("
                        + "ecchronos_id TEXT, "
                        + "datacenter_name TEXT, "
                        + "node_id UUID, "
                        + "node_endpoint TEXT, "
                        + "node_status TEXT, "
                        + "last_connection TIMESTAMP, "
                        + "next_connection TIMESTAMP, "
                        + "PRIMARY KEY(ecchronos_id, datacenter_name, node_id)) "
                        + "WITH CLUSTERING ORDER BY(datacenter_name DESC, node_id DESC);",
                KEYSPACE, TABLE));
    }

    @After
    public void cleanup()
    {
        mySession.execute(SimpleStatement.newInstance(
                String.format("TRUNCATE %s.%s", KEYSPACE, TABLE)));
    }

    /**
     * Without a self-healing mechanism, rows of a decommissioned instance persist as
     * {@code AVAILABLE} with no TTL, and re-adding the datacenter accumulates orphaned rows.
     */
    @Test
    public void rowsWithoutTtlPersistIndefinitelyForDecommissionedInstance()
    {
        // Two live instances, each populating its own partition with AVAILABLE rows.
        UUID dc1Node1 = UUID.randomUUID();
        UUID dc1Node2 = UUID.randomUUID();
        UUID dc3Node1 = UUID.randomUUID();
        UUID dc3Node2 = UUID.randomUUID();

        insertRow(DC1_ID, DC1_NAME, dc1Node1, "10.0.1.1");
        insertRow(DC1_ID, DC1_NAME, dc1Node2, "10.0.1.2");
        insertRow(DC3_ID, DC3_NAME, dc3Node1, "10.0.3.1");
        insertRow(DC3_ID, DC3_NAME, dc3Node2, "10.0.3.2");

        assertThat(countRows(DC1_ID)).isEqualTo(2);
        assertThat(countRows(DC3_ID)).isEqualTo(2);

        // dc3 is permanently decommissioned: nothing ever touches the dc3-ecchronos partition
        // again (no delete, no status update), modelled by simply not using dc3's EccNodesSync.
        // The surviving dc1 instance only ever deletes within its own partition and rejects
        // nodes from other datacenters, so dc3's partition is orphaned.
        List<Row> dc3Rows = fetchRows(DC3_ID);

        assertThat(dc3Rows)
                .as("dc3-ecchronos rows should still be present (never cleaned up without TTL)")
                .hasSize(2);
        for (Row row : dc3Rows)
        {
            assertThat(row.getString("node_status"))
                    .as("dc3 node still reported AVAILABLE despite being decommissioned")
                    .isEqualTo(NodeStatus.AVAILABLE.name());
        }

        // TTL(node_status) is null => the rows never expire on their own.
        assertThat(fetchNodeStatusTtls(DC3_ID))
                .as("dc3 rows have no TTL (they never expire)")
                .hasSize(2)
                .allSatisfy(ttl -> assertThat(ttl)
                        .as("TTL(node_status) should be null for a stale AVAILABLE row")
                        .isNull());

        // Re-adding dc3 with fresh host IDs creates NEW rows without removing the old orphans.
        insertRow(DC3_ID, DC3_NAME, UUID.randomUUID(), "10.0.3.101");
        insertRow(DC3_ID, DC3_NAME, UUID.randomUUID(), "10.0.3.102");

        assertThat(countRows(DC3_ID))
                .as("after decommission + re-add, dc3 leaks 2 stale rows (4 total for 2 live nodes)")
                .isEqualTo(4);
    }

    /**
     * A heartbeat with TTL makes an orphaned row self-heal. While the owning instance renews the
     * row it survives; once the instance is gone (renewal stops), the WHOLE row expires via its
     * TTL. This exercises the real {@link EccNodesSync} heartbeat write path.
     */
    @Test
    public void heartbeatMakesDecommissionedRowsSelfHeal() throws IOException
    {
        EccNodesSync dc3Sync = newSync(DC3_ID);
        UUID dc3Node1 = UUID.randomUUID();
        UUID dc3Node2 = UUID.randomUUID();
        int shortTtl = 3;

        // dc3 instance alive: it renews its rows a few times -> rows stay present.
        for (int i = 0; i < 3; i++)
        {
            dc3Sync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC3_NAME, "10.0.3.1", dc3Node1, shortTtl);
            dc3Sync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC3_NAME, "10.0.3.2", dc3Node2, shortTtl);
            assertThat(countRows(DC3_ID)).as("rows alive while renewed").isEqualTo(2);
            sleepSeconds(1);
        }

        // dc3 permanently decommissioned: renewal stops. Rows must disappear on their own.
        await().atMost(shortTtl + 10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> countRows(DC3_ID) == 0);

        assertThat(countRows(DC3_ID))
                .as("orphaned dc3 rows self-heal (expire) once heartbeat stops")
                .isEqualTo(0);
    }

    /**
     * Self-healing is independent of the connection awareness type (datacenterAware, rackAware,
     * hostAware). The awareness type only influences <em>which</em> nodes an instance owns
     * (via the node filter); it does not change how rows are written, renewed or expired. The
     * heartbeat + TTL mechanism operates purely on the instance's own {@code ecchronos_id}
     * partition regardless of type. This test renews rows shaped like rack-aware and host-aware
     * ownership (distinct racks/endpoints within a datacenter) and verifies the whole rows still
     * self-heal once renewal stops.
     */
    @Test
    public void selfHealingIsIndependentOfAwarenessType() throws IOException
    {
        // A single instance owning nodes across two racks (rackAware) and specific hosts
        // (hostAware). The rows differ only in data (endpoint/rack); the write path is identical.
        String instanceId = "rack-host-aware-ecchronos";
        EccNodesSync sync = newSync(instanceId);
        UUID rack1Node = UUID.randomUUID();
        UUID rack2Node = UUID.randomUUID();
        int shortTtl = 3;

        for (int i = 0; i < 3; i++)
        {
            // Same datacenter, different racks / host endpoints - representative of rackAware
            // and hostAware ownership.
            sync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC1_NAME, "10.0.1.11", rack1Node, shortTtl);
            sync.updateNodeHeartbeat(NodeStatus.AVAILABLE, DC1_NAME, "10.0.2.11", rack2Node, shortTtl);
            assertThat(countRows(instanceId)).as("rows alive while renewed").isEqualTo(2);
            sleepSeconds(1);
        }

        // Instance removed -> renewal stops -> whole rows expire, exactly as for datacenterAware.
        await().atMost(shortTtl + 10, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> countRows(instanceId) == 0);

        assertThat(countRows(instanceId))
                .as("self-healing works regardless of awareness type")
                .isEqualTo(0);
    }

    /**
     * Before the TTL removes the row, a missed-heartbeat row is surfaced as stale on the read
     * path (node_status preserved, derived 'stale' flag = true).
     */
    @Test
    public void staleIsDerivedFromLastConnectionOnReadPath()
    {
        UUID nodeId = UUID.randomUUID();
        // last_connection well in the past; node_status still AVAILABLE.
        mySession.execute(SimpleStatement.newInstance(String.format(
                "INSERT INTO %s.%s (ecchronos_id, datacenter_name, node_id, node_endpoint, "
                        + "node_status, last_connection, next_connection) VALUES (?, ?, ?, ?, ?, ?, ?)",
                KEYSPACE, TABLE),
                DC3_ID, DC3_NAME, nodeId, "10.0.3.1", NodeStatus.AVAILABLE.name(),
                Instant.now().minus(10, ChronoUnit.MINUTES), Instant.now()));

        Row row = fetchRows(DC3_ID).get(0);

        // Threshold 2 minutes: a 10-minute-old row is stale, but node_status is untouched.
        NodeSyncState stale = NodeSyncState.fromRow(row, TimeUnit.MINUTES.toMillis(2));
        assertThat(stale.stale()).isTrue();
        assertThat(stale.nodeStatus()).isEqualTo(NodeStatus.AVAILABLE.name());

        // A fresh row (just written) is not stale.
        UUID freshId = UUID.randomUUID();
        mySession.execute(SimpleStatement.newInstance(String.format(
                "INSERT INTO %s.%s (ecchronos_id, datacenter_name, node_id, node_endpoint, "
                        + "node_status, last_connection, next_connection) VALUES (?, ?, ?, ?, ?, ?, ?)",
                KEYSPACE, TABLE),
                DC1_ID, DC1_NAME, freshId, "10.0.1.1", NodeStatus.AVAILABLE.name(),
                Instant.now(), Instant.now()));
        Row freshRow = fetchRows(DC1_ID).get(0);
        NodeSyncState fresh = NodeSyncState.fromRow(freshRow, TimeUnit.MINUTES.toMillis(2));
        assertThat(fresh.stale()).isFalse();
    }

    private EccNodesSync newSync(final String ecchronosId) throws IOException
    {
        DistributedNativeConnectionProvider provider = getNativeConnectionProvider();
        return EccNodesSync.newBuilder()
                .withSession(mySession)
                .withNativeConnection(provider)
                .withConnectionDelayValue(Long.valueOf(30))
                .withConnectionDelayUnit(TimeUnit.MINUTES)
                .withEcchronosID(ecchronosId)
                .build();
    }

    private void sleepSeconds(final int seconds)
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

    private void insertRow(final String ecchronosId, final String dcName, final UUID nodeId,
                           final String endpoint)
    {
        mySession.execute(SimpleStatement.newInstance(String.format(
                "INSERT INTO %s.%s (ecchronos_id, datacenter_name, node_id, node_endpoint, "
                        + "node_status, last_connection, next_connection) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)", KEYSPACE, TABLE),
                ecchronosId, dcName, nodeId, endpoint, NodeStatus.AVAILABLE.name(),
                Instant.now(), Instant.now().plus(30, ChronoUnit.MINUTES)));
    }

    private long countRows(final String ecchronosId)
    {
        return fetchRows(ecchronosId).size();
    }

    private List<Row> fetchRows(final String ecchronosId)
    {
        ResultSet rs = mySession.execute(SimpleStatement.newInstance(
                String.format("SELECT * FROM %s.%s WHERE ecchronos_id = ?", KEYSPACE, TABLE),
                ecchronosId));
        List<Row> rows = new ArrayList<>();
        rs.forEach(rows::add);
        return rows;
    }

    private List<Integer> fetchNodeStatusTtls(final String ecchronosId)
    {
        ResultSet rs = mySession.execute(SimpleStatement.newInstance(
                String.format("SELECT TTL(node_status) AS status_ttl FROM %s.%s WHERE ecchronos_id = ?",
                        KEYSPACE, TABLE),
                ecchronosId));
        List<Integer> ttls = new ArrayList<>();
        for (Row row : rs)
        {
            ttls.add(row.isNull("status_ttl") ? null : row.getInt("status_ttl"));
        }
        return ttls;
    }
}
