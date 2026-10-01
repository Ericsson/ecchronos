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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNodeSyncState
{
    private Row mockRow(final Instant lastConnection)
    {
        Row row = mock(Row.class);
        when(row.getString(NodeSyncState.COLUMN_ECCHRONOS_ID)).thenReturn("ecc-1");
        when(row.getString(NodeSyncState.COLUMN_DATACENTER_NAME)).thenReturn("dc1");
        when(row.getUuid(NodeSyncState.COLUMN_NODE_ID)).thenReturn(UUID.randomUUID());
        when(row.getInstant(NodeSyncState.COLUMN_LAST_CONNECTION)).thenReturn(lastConnection);
        when(row.getInstant(NodeSyncState.COLUMN_NEXT_CONNECTION)).thenReturn(lastConnection);
        when(row.getString(NodeSyncState.COLUMN_NODE_ENDPOINT)).thenReturn("10.0.0.1");
        when(row.getString(NodeSyncState.COLUMN_NODE_STATUS)).thenReturn("AVAILABLE");
        return row;
    }

    @Test
    public void freshRowIsNotStale()
    {
        Row row = mockRow(Instant.now());
        NodeSyncState state = NodeSyncState.fromRow(row, 120_000L);
        assertThat(state.stale()).isFalse();
        assertThat(state.nodeStatus()).isEqualTo("AVAILABLE");
        // Age is always exposed, even for fresh rows (small, non-negative).
        assertThat(state.lastHeartbeatAgeMs()).isGreaterThanOrEqualTo(0L).isLessThan(120_000L);
    }

    @Test
    public void oldRowIsStaleWhenThresholdExceeded()
    {
        Row row = mockRow(Instant.now().minus(10, ChronoUnit.MINUTES));
        NodeSyncState state = NodeSyncState.fromRow(row, 120_000L);
        assertThat(state.stale()).isTrue();
        // node_status is preserved untouched; only the derived flag changes.
        assertThat(state.nodeStatus()).isEqualTo("AVAILABLE");
        // Age reflects ~10 minutes since the last heartbeat.
        assertThat(state.lastHeartbeatAgeMs()).isGreaterThanOrEqualTo(TimeUnit.MINUTES.toMillis(10));
    }

    @Test
    public void thresholdZeroDisablesStalenessButStillExposesAge()
    {
        Row row = mockRow(Instant.now().minus(10, ChronoUnit.MINUTES));
        NodeSyncState state = NodeSyncState.fromRow(row, 0L);
        assertThat(state.stale()).isFalse();
        assertThat(state.lastHeartbeatAgeMs()).isGreaterThanOrEqualTo(TimeUnit.MINUTES.toMillis(10));
    }

    @Test
    public void defaultFromRowIsNeverStale()
    {
        Row row = mockRow(Instant.now().minus(10, ChronoUnit.MINUTES));
        NodeSyncState state = NodeSyncState.fromRow(row);
        assertThat(state.stale()).isFalse();
    }
}
