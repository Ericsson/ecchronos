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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResource;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairResourceFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;

/**
 * Repair resource factory for single session repair: the repair types PRIMARY_RANGE and ALL_REPLICATED_RANGES.
 * <p>
 * Both repair a node in a single repair session, the primary ranges like {@code nodetool repair -pr} and everything the
 * node replicates like {@code nodetool repair}, so they take the same locks. It produces two kinds of single slot locks:
 * <ul>
 *     <li>A table lock, {@code keyspace.table}, which is the same lock incremental repair takes. Two single session
 *     repairs of the same table, and a single session repair and an incremental repair of the same table, can
 *     therefore not run at the same time, regardless of datacenter.</li>
 *     <li>One lock per datacenter involved in the repair. It has its own name prefix, so it is a different lock than
 *     the datacenter lock vnode repair takes (named after the datacenter only). Single session repair and vnode
 *     repair are intentionally not coordinated with each other. The prefix is {@code PrimaryRange-}, after the first
 *     repair type that used it. It is the name of a lock in the lock table and must not change.</li>
 * </ul>
 * Locks are never taken per node. The locks of one repair are acquired together, all or nothing.
 */
public class SingleSessionRepairResourceFactory implements RepairResourceFactory
{
    /**
     * Datacenter placeholder for the table lock, shared with {@link IncrementalRepairResourceFactory}. The lock
     * identity is the resource name alone, so this only needs to be stable and datacenter independent.
     */
    private static final String GLOBAL_TABLE_LOCK_DC = "global";

    /**
     * Prefix of the datacenter lock names. Keeps the lock apart from the vnode datacenter lock, whose name is the
     * datacenter name only, and from node and table locks.
     */
    static final String DATA_CENTER_LOCK_PREFIX = "PrimaryRange-";

    private static final int SINGLE_SLOT = 1;

    private final TableReference myTableReference;

    /**
     * Constructor.
     *
     * @param tableReference the table being repaired; used to build the table lock resource.
     */
    public SingleSessionRepairResourceFactory(final TableReference tableReference)
    {
        myTableReference = Preconditions.checkNotNull(tableReference, "Table reference must be set");
    }

    @Override
    public final Set<RepairResource> getRepairResources(final ReplicaRepairGroup replicaRepairGroup)
    {
        Set<RepairResource> repairResources = new HashSet<>();

        // Same name and slot count as the incremental table lock, so the two exclude each other per table.
        String tableResourceName = myTableReference.getKeyspace() + "." + myTableReference.getTable();
        repairResources.add(new RepairResource(GLOBAL_TABLE_LOCK_DC, tableResourceName, SINGLE_SLOT));

        for (String dataCenter : replicaRepairGroup.getDataCenters())
        {
            repairResources.add(new RepairResource(dataCenter, DATA_CENTER_LOCK_PREFIX + dataCenter, SINGLE_SLOT));
        }

        return repairResources;
    }
}
