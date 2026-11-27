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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.singlesession;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicationState;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.collect.ImmutableSet;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the token ranges of a keyspace that a repair of type
 * {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType#ALL_REPLICATED_RANGES} does not reach,
 * and warns about them.
 * <p>
 * That repair type repairs every range the managed nodes replicate, and nothing else. A range that none of the nodes
 * managed by this ecChronos instance replicates is therefore not repaired, which happens for example when the
 * instance manages a few nodes of a large datacenter, or only a datacenter that does not replicate the keyspace.
 * Nothing else reports it, so it is reported here, once per keyspace and not for every table and node.
 * <p>
 * The check is made on replica membership: a range is reached when at least one of its replicas is a managed node.
 * It does not depend on how ranges are split or merged per node.
 * <p>
 * The warning is repeated after {@link #REWARN_INTERVAL_MS} for as long as the same ranges are missing, and at once
 * when the missing ranges change. A keyspace that is fully covered again is forgotten, so a later gap is reported
 * at once.
 */
public final class ReplicatedRangesCoverage
{
    /** How long to wait before the same warning of a keyspace is logged again. */
    static final long REWARN_INTERVAL_MS = 60L * 60L * 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(ReplicatedRangesCoverage.class);
    private static final int EXAMPLE_RANGES = 3;

    private final ReplicationState myReplicationState;
    private final Supplier<Collection<Node>> myManagedNodes;
    private final Clock myClock;
    private final Map<String, Warned> myWarned = new ConcurrentHashMap<>();

    /**
     * Constructs a coverage check.
     *
     * @param replicationState the replication state, that tells which nodes replicate the ranges of a keyspace.
     * @param managedNodes the nodes managed by this ecChronos instance, read each time the check is made.
     */
    public ReplicatedRangesCoverage(final ReplicationState replicationState, final Supplier<Collection<Node>> managedNodes)
    {
        this(replicationState, managedNodes, Clock.systemUTC());
    }

    ReplicatedRangesCoverage(final ReplicationState replicationState,
            final Supplier<Collection<Node>> managedNodes,
            final Clock clock)
    {
        myReplicationState = replicationState;
        myManagedNodes = managedNodes;
        myClock = clock;
    }

    /**
     * The ranges of the keyspace of the table that none of the managed nodes replicates.
     *
     * @param currentNode a node to read the token metadata for.
     * @param tableReference the table, which gives the keyspace.
     * @return the ranges that are not reached, empty if every range is.
     */
    public Set<LongTokenRange> uncoveredRanges(final Node currentNode, final TableReference tableReference)
    {
        Set<UUID> managed = new HashSet<>();
        for (Node node : myManagedNodes.get())
        {
            managed.add(node.getHostId());
        }
        Set<LongTokenRange> uncovered = new HashSet<>();
        for (Map.Entry<LongTokenRange, ImmutableSet<DriverNode>> range
                : myReplicationState.getTokenRanges(tableReference, currentNode).entrySet())
        {
            if (range.getValue().stream().noneMatch(replica -> managed.contains(replica.getId())))
            {
                uncovered.add(range.getKey());
            }
        }
        return uncovered;
    }

    /**
     * Warn if some ranges of the keyspace of the table are not reached by the managed nodes. It never throws: a
     * repair must not fail because this check could not be made.
     *
     * @param currentNode a node to read the token metadata for.
     * @param tableReference the table, which gives the keyspace.
     */
    public void warnIfUncovered(final Node currentNode, final TableReference tableReference)
    {
        String keyspace = tableReference.getKeyspace();
        try
        {
            Set<LongTokenRange> uncovered = uncoveredRanges(currentNode, tableReference);
            if (uncovered.isEmpty())
            {
                myWarned.remove(keyspace);
                return;
            }
            long now = myClock.millis();
            Warned previous = myWarned.get(keyspace);
            if (previous != null && previous.uncovered().equals(uncovered)
                    && now - previous.warnedAt() < REWARN_INTERVAL_MS)
            {
                return;
            }
            myWarned.put(keyspace, new Warned(uncovered, now));
            LOG.warn("Keyspace {}: {} of its token ranges are not replicated by any node managed by this ecChronos "
                    + "instance, so repair type ALL_REPLICATED_RANGES does not repair them. For example {}. Manage "
                    + "the nodes that replicate them, or use another repair type.",
                    keyspace, uncovered.size(), examples(uncovered));
        }
        catch (RuntimeException e)
        {
            LOG.debug("Unable to check which token ranges of keyspace {} are reached by the managed nodes",
                    keyspace, e);
        }
    }

    private static List<LongTokenRange> examples(final Set<LongTokenRange> ranges)
    {
        List<LongTokenRange> sorted = new ArrayList<>(ranges);
        sorted.sort((a, b) -> Long.compare(a.start, b.start));
        return sorted.subList(0, Math.min(EXAMPLE_RANGES, sorted.size()));
    }

    private record Warned(Set<LongTokenRange> uncovered, long warnedAt)
    {
    }
}
