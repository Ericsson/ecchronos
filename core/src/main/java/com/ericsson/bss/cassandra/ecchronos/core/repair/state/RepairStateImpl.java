/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.repair.state;

import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RepairStateImpl implements RepairState
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairStateImpl.class);

    private static final ThreadLocal<SimpleDateFormat> MY_DATE_FORMAT
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US));

    private final AtomicReference<RepairStateSnapshot> myRepairStateSnapshot = new AtomicReference<>();

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final VnodeRepairStateFactory myVnodeRepairStateFactory;
    private final HostStates myHostStates;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ReplicaRepairGroupFactory myReplicaRepairGroupFactory;
    private final PostUpdateHook myPostUpdateHook;

    public RepairStateImpl(final TableReference tableReference,
                           final RepairConfiguration repairConfiguration,
                           final VnodeRepairStateFactory vnodeRepairStateFactory,
                           final HostStates hostStates,
                           final TableRepairMetrics tableRepairMetrics,
                           final ReplicaRepairGroupFactory replicaRepairGroupFactory,
                           final PostUpdateHook postUpdateHook)
    {
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
        if (oldRepairStateSnapshot == null
                || isRepairNeeded(oldRepairStateSnapshot.lastCompletedAt(),
                oldRepairStateSnapshot.getEstimatedRepairTime(),
                System.currentTimeMillis()))
        {
            RepairStateSnapshot newRepairStateSnapshot = generateNewRepairState(oldRepairStateSnapshot);
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

    private RepairStateSnapshot generateNewRepairState(final RepairStateSnapshot old)
    {
        VnodeRepairStates vnodeRepairStates = myVnodeRepairStateFactory.calculateNewState(myTableReference, old);

        return generateSnapshotForVnode(vnodeRepairStates, old);
    }

    private RepairStateSnapshot generateSnapshotForVnode(final VnodeRepairStates vnodeRepairStates,
                                                         final RepairStateSnapshot old)
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
                    MY_DATE_FORMAT.get().format(new Date(repairedAt)), MY_DATE_FORMAT.get().format(new Date(next)));
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
                    MY_DATE_FORMAT.get().format(new Date(repairedAt)), MY_DATE_FORMAT.get().format(new Date(next)));
        }

        return repairedAt;
    }

    private long newTableRepairedAt()
    {
        long runIntervalInMs = myRepairConfiguration.getRepairIntervalInMs();
        long minimumRepairWait = Math.min(runIntervalInMs, TimeUnit.DAYS.toMillis(1));
        long assumedRepairedAt = System.currentTimeMillis() - runIntervalInMs + minimumRepairWait;

        if (LOG.isInfoEnabled())
        {
            LOG.info("Assuming the table {} is new, next repair {}",
                    myTableReference,
                    MY_DATE_FORMAT.get().format(new Date(assumedRepairedAt + runIntervalInMs)));
        }

        return assumedRepairedAt;
    }

    private boolean replicasAreRepairable(final VnodeRepairState vnodeRepairState)
    {
        for (DriverNode node : vnodeRepairState.getReplicas())
        {
            if (!myHostStates.isUp(node))
            {
                LOG.trace("{} not repairable, host {} is not up", vnodeRepairState, node);
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
        return lastRepairedAt + (myRepairConfiguration.getRepairIntervalInMs() - estimatedRepairTime) <= now;
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
