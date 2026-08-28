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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.NodeSyncState;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * Internal State REST interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface StateManagementREST
{
    /**
     * Get a list of nodes managed by ecChronos instances.
     *
     * @param datacenter The datacenter name (optional). Only applies when all is false.
     * @param all When true, return nodes from all ecChronos instances. When false, return nodes for the local instance.
     * @return A list of JSON representations of {@link NodeSyncState}
     */
    ResponseEntity<List<NodeSyncState>> getNodes(String datacenter, boolean all);
}
