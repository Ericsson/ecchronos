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

import java.util.Objects;
import java.util.UUID;

/**
 * A representation of a Cassandra incremental repair session, as reported by the {@code RepairService} MBean
 * {@code getSessions} operation on a specific managed node.
 *
 * Primarily used to have a type to convert to JSON.
 */
@SuppressWarnings("VisibilityModifier")
public class RepairSession
{
    /** The managed node that reported this session. */
    public UUID nodeID;
    /** The repair session id. */
    public String sessionId;
    /** The session state (e.g. REPAIRING, FINALIZED, FAILED). */
    public String state;
    /** The coordinator address as reported by Cassandra. */
    public String coordinator;
    /** The epoch-seconds timestamp when the session started, or -1 if unknown. */
    public long started;
    /** The epoch-seconds timestamp of the last session update, or -1 if unknown. */
    public long lastUpdate;
    /** The comma-separated participant host addresses. */
    public String participants;
    /** The comma-separated {@code keyspace.table} names. */
    public String tables;

    /** Constructs a new RepairSession. */
    public RepairSession()
    {
    }

    /**
     * Constructs a new RepairSession.
     *
     * @param theNodeID the managed node that reported the session.
     * @param theSessionId the session id.
     * @param theState the session state.
     * @param theCoordinator the coordinator address.
     * @param theStarted the epoch-seconds started timestamp, or -1 if unknown.
     * @param theLastUpdate the epoch-seconds last-update timestamp, or -1 if unknown.
     * @param theParticipants the participant host addresses.
     * @param theTables the affected {@code keyspace.table} names.
     */
    @SuppressWarnings("checkstyle:parameternumber")
    public RepairSession(final UUID theNodeID, final String theSessionId, final String theState,
            final String theCoordinator, final long theStarted, final long theLastUpdate,
            final String theParticipants, final String theTables)
    {
        this.nodeID = theNodeID;
        this.sessionId = theSessionId;
        this.state = theState;
        this.coordinator = theCoordinator;
        this.started = theStarted;
        this.lastUpdate = theLastUpdate;
        this.participants = theParticipants;
        this.tables = theTables;
    }

    /**
     * Checks equality.
     *
     * @param o the other object.
     * @return true if equal.
     */
    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        RepairSession that = (RepairSession) o;
        return started == that.started
                && lastUpdate == that.lastUpdate
                && Objects.equals(nodeID, that.nodeID)
                && Objects.equals(sessionId, that.sessionId)
                && Objects.equals(state, that.state)
                && Objects.equals(coordinator, that.coordinator)
                && Objects.equals(participants, that.participants)
                && Objects.equals(tables, that.tables);
    }

    /**
     * Computes the hash code.
     *
     * @return the hash code.
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(nodeID, sessionId, state, coordinator, started, lastUpdate, participants, tables);
    }
}
