/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.state;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.state.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.state.PostUpdateHook;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairedAt;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroup;
import com.ericsson.bss.cassandra.ecchronos.core.state.ReplicaRepairGroupFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStateFactory;
import com.ericsson.bss.cassandra.ecchronos.core.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableRepairMetrics;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RepairStateImpl implements RepairState
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairStateImpl.class);

    private static final DateTimeFormatter MY_DATE_FORMAT = DateTimeFormatter.ofPattern(
            "yyyy-MM-dd HH:mm:ss", Locale.US);

    private final AtomicReference<RepairStateSnapshot> myRepairStateSnapshot = new AtomicReference<>();

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final VnodeRepairStateFactory myVnodeRepairStateFactory;
    private final HostStates myHostStates;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ReplicaRepairGroupFactory myReplicaRepairGroupFactory;
    private final PostUpdateHook myPostUpdateHook;
    private final Node myNode;

    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public RepairStateImpl(
            final Node node,
            final TableReference tableReference,
            final RepairConfiguration repairConfiguration,
            final VnodeRepairStateFactory vnodeRepairStateFactory,
            final HostStates hostStates,
            final TableRepairMetrics tableRepairMetrics,
            final ReplicaRepairGroupFactory replicaRepairGroupFactory,
            final PostUpdateHook postUpdateHook)
    {
        myNode = node;
        myTableReference = tableReference;
        myRepairConfiguration = repairConfiguration;
        myVnodeRepairStateFactory = vnodeRepairStateFactory;
        myHostStates = hostStates;
        myTableRepairMetrics = tableRepairMetrics;
        myReplicaRepairGroupFactory = replicaRepairGroupFactory;
        myPostUpdateHook = postUpdateHook;

        update();
    }

    @Override
    public final void update()
    {
        RepairStateSnapshot oldRepairStateSnapshot = myRepairStateSnapshot.get();
        long now = System.currentTimeMillis();
        if (oldRepairStateSnapshot == null
                || isRepairNeeded(oldRepairStateSnapshot.lastCompletedAt(),
                oldRepairStateSnapshot.getEstimatedRepairTime(),
                now))
        {
            RepairStateSnapshot newRepairStateSnapshot = generateNewRepairState(myNode, oldRepairStateSnapshot, now);
            if (myRepairStateSnapshot.compareAndSet(oldRepairStateSnapshot, newRepairStateSnapshot))
            {
                myTableRepairMetrics.lastRepairedAt(myTableReference, newRepairStateSnapshot.lastCompletedAt());

                int nonRepairedRanges
                        = (int) newRepairStateSnapshot.getVnodeRepairStates().getVnodeRepairStates().stream()
                        .filter(v -> vnodeIsRepairable(v, newRepairStateSnapshot, System.currentTimeMillis()))
                        .count();

                int repairedRanges
                        = newRepairStateSnapshot.getVnodeRepairStates().getVnodeRepairStates().size()
                        - nonRepairedRanges;
                myTableRepairMetrics.repairState(myTableReference, repairedRanges, nonRepairedRanges);
                myTableRepairMetrics.remainingRepairTime(myTableReference,
                        newRepairStateSnapshot.getRemainingRepairTime(System.currentTimeMillis(),
                                myRepairConfiguration.getRepairIntervalInMs()));
                LOG.trace("Table {} switched to repair state {}", myTableReference, newRepairStateSnapshot);
            }
        }
        else
        {
            LOG.trace("Table {} keeping repair state {}", myTableReference, oldRepairStateSnapshot);
        }
        myPostUpdateHook.postUpdate(myRepairStateSnapshot.get());
    }

    /**
     * Returns the repair state snapshot.
     *
     * @return RepairStateSnapshot
     */
    @Override
    public RepairStateSnapshot getSnapshot()
    {
        return myRepairStateSnapshot.get();
    }

    private RepairStateSnapshot generateNewRepairState(final Node node, final RepairStateSnapshot old, final long now)
    {
        VnodeRepairStates vnodeRepairStates = myVnodeRepairStateFactory.calculateNewState(node, myTableReference, old, now);

        return generateSnapshotForVnode(vnodeRepairStates, old, now);
    }

    private RepairStateSnapshot generateSnapshotForVnode(final VnodeRepairStates vnodeRepairStates,
            final RepairStateSnapshot old, final long createdAt)
    {
        long repairedAt = calculateRepairedAt(vnodeRepairStates, old);

        VnodeRepairStates updatedVnodeRepairStates = vnodeRepairStates.combineWithRepairedAt(repairedAt);

        List<VnodeRepairState> repairableVnodes = updatedVnodeRepairStates.getVnodeRepairStates().stream()
                .filter(this::replicasAreRepairable)
                .filter(v -> vnodeIsRepairable(v, old, System.currentTimeMillis()))
                .collect(Collectors.toList());

        List<ReplicaRepairGroup> replicaRepairGroups
                = myReplicaRepairGroupFactory.generateReplicaRepairGroups(repairableVnodes);

        return RepairStateSnapshot.newBuilder()
                .withLastCompletedAt(repairedAt)
                .withVnodeRepairStates(updatedVnodeRepairStates)
                .withCreatedAt(createdAt)
                .withReplicaRepairGroups(replicaRepairGroups)
                .build();
    }

    private long calculateRepairedAt(final VnodeRepairStates vnodeRepairStates, final RepairStateSnapshot old)
    {
        RepairedAt repairedAt = RepairedAt.generate(vnodeRepairStates);
        LOG.trace("RepairedAt: {}, calculated from: {}", repairedAt, vnodeRepairStates);

        long calculatedRepairedAt;

        if (!repairedAt.isRepaired())
        {
            if (repairedAt.isPartiallyRepaired())
            {
                calculatedRepairedAt = partiallyRepairedTableRepairedAt(repairedAt.getMaxRepairedAt(), old);
            }
            else
            {
                calculatedRepairedAt = newTableRepairedAt();
            }
        }
        else
        {
            calculatedRepairedAt = repairedTableRepairedAt(repairedAt.getMinRepairedAt(), old);
        }
        return calculatedRepairedAt;
    }

    private long repairedTableRepairedAt(final long repairedAt, final RepairStateSnapshot old)
    {
        if (LOG.isDebugEnabled())
        {
            long next = repairedAt + myRepairConfiguration.getRepairIntervalInMs();
            if (old != null)
            {
                next -= old.getEstimatedRepairTime();
            }
            LOG.debug("Table {} fully repaired at {}, next repair at/after {}", myTableReference,
                    LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds(repairedAt), 0,
                            ZoneOffset.ofHours(0)).format(MY_DATE_FORMAT),
                    LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds(next), 0,
                            ZoneOffset.ofHours(0)).format(MY_DATE_FORMAT));
        }
        return repairedAt;
    }

    private long partiallyRepairedTableRepairedAt(final long maxRepairedAt, final RepairStateSnapshot old)
    {
        long runIntervalInMs = myRepairConfiguration.getRepairIntervalInMs();
        long repairedAt = Math.min(System.currentTimeMillis() - runIntervalInMs, maxRepairedAt);
        if (LOG.isDebugEnabled())
        {
            long next = repairedAt + runIntervalInMs;
            if (old != null)
            {
                next -= old.getEstimatedRepairTime();
            }
            LOG.debug("Table {} partially repaired at {}, next repair at/after {}", myTableReference,
                    LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds(repairedAt), 0,
                            ZoneOffset.ofHours(0)).format(MY_DATE_FORMAT),
                    LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds(next), 0,
                            ZoneOffset.ofHours(0)).format(MY_DATE_FORMAT));
        }

        return repairedAt;
    }

    private long newTableRepairedAt()
    {
        long runIntervalInMs = myRepairConfiguration.getRepairIntervalInMs();
        long initialDelayInMs = myRepairConfiguration.getInitialDelayInMs();
        long assumedRepairedAt = System.currentTimeMillis() - runIntervalInMs + initialDelayInMs;
        LOG.info("Assuming the table {} is new. Next repair will occur at {}.",
                myTableReference,
                LocalDateTime.ofEpochSecond(TimeUnit.MILLISECONDS.toSeconds(assumedRepairedAt + runIntervalInMs), 0,
                        ZoneOffset.ofHours(0)).format(MY_DATE_FORMAT));
        return assumedRepairedAt;
    }

    private boolean replicasAreRepairable(final VnodeRepairState vnodeRepairState)
    {
        for (DriverNode node : vnodeRepairState.getReplicas())
        {
            if (!myHostStates.isUp(node))
            {
                LOG.trace("{} not repairable, host {} is NOT UP", vnodeRepairState, node);
                return false;
            }
        }
        return true;
    }

    /**
     * Returns if repair is needed. If the job's estimated finished time has passed, it is up for repair.
     *
     * @param lastRepairedAt Time when last repaired.
     * @param estimatedRepairTime The estimated repair time.
     * @param now Current time.
     * @return boolean
     */
    @VisibleForTesting
    boolean isRepairNeeded(final long lastRepairedAt, final long estimatedRepairTime, final long now)
    {
        boolean isRepairNeeded = lastRepairedAt + (myRepairConfiguration.getRepairIntervalInMs() - estimatedRepairTime) <= now;
        if (LOG.isDebugEnabled())
        {
            String message;
            if (isRepairNeeded)
            {
                message = "{} is ready for Repair. Time since Last Repaired {} estimated repair time {} Repair Interval {}";
            }
            else
            {
                message = "{} is not ready for Repair. Time since Last Repaired {} estimated repair time {} Repair Interval {}";
            }
            LOG.debug(message, myTableReference.getTable(), (now - lastRepairedAt) / 1000, estimatedRepairTime / 1000,
                    myRepairConfiguration.getRepairIntervalInMs() / 1000);
        }
        return isRepairNeeded;
    }

    private boolean vnodeIsRepairable(final VnodeRepairState vnodeRepairState,
            final RepairStateSnapshot snapshot,
            final long now)
    {
        long estimatedRepairTime = 0L;
        if (snapshot != null)
        {
            estimatedRepairTime = snapshot.getEstimatedRepairTime();
        }
        return isRepairNeeded(vnodeRepairState.lastRepairedAt(), estimatedRepairTime, now);
    }
}

