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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairSession;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * Repair Sessions REST interface for listing and manually failing incremental repair sessions.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface RepairSessionsREST
{
    /**
     * List incremental repair sessions across managed nodes.
     *
     * @param nodeID Only list sessions reported by this managed node (optional).
     * @return A list of JSON representations of {@link RepairSession}.
     */
    ResponseEntity<List<RepairSession>> getRepairSessions(String nodeID);

    /**
     * Fail (cancel) an incremental repair session.
     * <p>
     * With {@code force=false} the session is cancelled on the managed node that is its coordinator. With
     * {@code force=true} it is force-failed on every managed node that reports the session.
     *
     * @param sessionId The id of the session to fail.
     * @param force Whether to force-fail on all participant nodes rather than only the coordinator.
     * @param nodeID Restrict the operation to a single managed node (optional).
     * @return A list of JSON representations of the {@link RepairSession} entries that were failed.
     */
    ResponseEntity<List<RepairSession>> failRepairSession(String sessionId, boolean force, String nodeID);
}
