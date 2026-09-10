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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResourceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;

/**
 * Repair resource factory for incremental repair.
 * <p>
 * Incremental repair is a table-level operation: the whole owned dataset for the table is anticompacted, so two
 * incremental repairs of the same table must not run concurrently even across different replica nodes. At the same
 * time, incremental repairs of different tables should still be able to run in parallel, bounded per node by the
 * globally configured locks-per-resource value.
 * <p>
 * To achieve this the factory produces two independent lock dimensions:
 * <ul>
 *     <li>A single, datacenter-independent table-level resource with one slot ({@code maxSlots = 1}) — the
 *     correctness lock that serializes incremental repairs of the same table across all its replicas. The resource
 *     name is {@code keyspace.table} so that identically named tables in different keyspaces do not collide.</li>
 *     <li>One per-node resource per replica using the globally configured slot count — the load-throttling
 *     lock that bounds how many incremental repairs a given node participates in at once.</li>
 * </ul>
 * Because {@link com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockFactory#getLock} acquires the whole set
 * of resources atomically (all-or-nothing with rollback), locking both dimensions together yields the desired
 * behavior.
 */
public class IncrementalRepairResourceFactory implements RepairResourceFactory
{
    /**
     * Datacenter placeholder for the table-level correctness lock. The distributed lock identity is the resource
     * name alone, so this value only needs to be stable and datacenter-independent to yield a single global lock
     * for a given table across all replicas and datacenters.
     */
    private static final String GLOBAL_TABLE_LOCK_DC = "global";

    private final TableReference myTableReference;

    /**
     * Constructor.
     *
     * @param tableReference the table being repaired; used to build the table-level correctness lock resource.
     */
    public IncrementalRepairResourceFactory(final TableReference tableReference)
    {
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
    }

    @Override
    public final Set<RepairResource> getRepairResources(final ReplicaRepairGroup replicaRepairGroup)
    {
        Set<RepairResource> repairResources = new HashSet<>();

        // Dimension 1: a single, datacenter-independent one-per-table correctness lock (single slot). Keyed by
        // keyspace.table so that same-named tables in different keyspaces remain distinct. Serializes same-table
        // repairs across all replicas regardless of datacenter.
        String tableResourceName = myTableReference.getKeyspace() + "." + myTableReference.getTable();
        repairResources.add(new RepairResource(GLOBAL_TABLE_LOCK_DC, tableResourceName, 1));

        // Dimension 2: per-node load lock, bounded by the global locks-per-resource value.
        for (DriverNode replica : replicaRepairGroup.replicas())
        {
            repairResources.add(new RepairResource(replica.getDatacenter(), replica.getId().toString()));
        }

        return repairResources;
    }
}
