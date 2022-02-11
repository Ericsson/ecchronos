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
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
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

    private static final ThreadLocal<SimpleDateFormat> myDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US));

    private final AtomicReference<RepairStateSnapshot> myRepairStateSnapshot = new AtomicReference<>();

    private final TableReference myTableReference;
    private final RepairConfiguration myRepairConfiguration;
    private final VnodeRepairStateFactory myVnodeRepairStateFactory;
    private final HostStates myHostStates;
    private final TableRepairMetrics myTableRepairMetrics;
    private final ReplicaRepairGroupFactory myReplicaRepairGroupFactory;
    private final PostUpdateHook myPostUpdateHook;

    public RepairStateImpl(TableReference tableReference, RepairConfiguration repairConfiguration,
                           VnodeRepairStateFactory vnodeRepairStateFactory, HostStates hostStates,
                           TableRepairMetrics tableRepairMetrics, ReplicaRepairGroupFactory replicaRepairGroupFactory,
                           PostUpdateHook postUpdateHook)
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
                || oldRepairStateSnapshot.lastCompletedAt() < System.currentTimeMillis() - myRepairConfiguration.getRepairIntervalInMs())
        {
            RepairStateSnapshot newRepairStateSnapshot = generateNewRepairState(oldRepairStateSnapshot);
            if (myRepairStateSnapshot.compareAndSet(oldRepairStateSnapshot, newRepairStateSnapshot))
            {
                myTableRepairMetrics.lastRepairedAt(myTableReference, newRepairStateSnapshot.lastCompletedAt());

                int nonRepairedRanges = (int)newRepairStateSnapshot.getVnodeRepairStates().getVnodeRepairStates().stream()
                        .filter(this::vnodeIsRepairable)
                        .count();

                int repairedRanges = newRepairStateSnapshot.getVnodeRepairStates().getVnodeRepairStates().size() - nonRepairedRanges;
                myTableRepairMetrics.repairState(myTableReference, repairedRanges, nonRepairedRanges);

                LOG.trace("Table {} switched to repair state {}", myTableReference, newRepairStateSnapshot);
            }
        }
        else
        {
            LOG.trace("Table {} keeping repair state {}", myTableReference, oldRepairStateSnapshot);
        }
        if (myPostUpdateHook != null)
        {
            myPostUpdateHook.postUpdate(myRepairStateSnapshot.get());
        }
    }

    @Override
    public RepairStateSnapshot getSnapshot()
    {
        return myRepairStateSnapshot.get();
    }

    private RepairStateSnapshot generateNewRepairState(RepairStateSnapshot old)
    {
        VnodeRepairStates vnodeRepairStates = myVnodeRepairStateFactory.calculateNewState(myTableReference, old);

        return generateSnapshotForVnode(vnodeRepairStates);
    }

    private RepairStateSnapshot generateSnapshotForVnode(VnodeRepairStates vnodeRepairStates)
    {
        long repairedAt = calculateRepairedAt(vnodeRepairStates);

        VnodeRepairStates updatedVnodeRepairStates = vnodeRepairStates.combineWithRepairedAt(repairedAt);

        List<VnodeRepairState> repairableVnodes = updatedVnodeRepairStates.getVnodeRepairStates().stream()
                .filter(this::replicasAreRepairable)
                .filter(this::vnodeIsRepairable)
                .collect(Collectors.toList());

        List<ReplicaRepairGroup> replicaRepairGroups = myReplicaRepairGroupFactory.generateReplicaRepairGroups(repairableVnodes);

        return RepairStateSnapshot.newBuilder()
                .withLastCompletedAt(repairedAt)
                .withVnodeRepairStates(updatedVnodeRepairStates)
                .withReplicaRepairGroups(replicaRepairGroups)
                .build();
    }

    private long calculateRepairedAt(VnodeRepairStates vnodeRepairStates)
    {
        RepairedAt repairedAt = RepairedAt.generate(vnodeRepairStates);
        LOG.trace("RepairedAt: {}, calculated from: {}", repairedAt, vnodeRepairStates);

        long calculatedRepairedAt = 0;

        if (!repairedAt.isRepaired())
        {
            if (repairedAt.isPartiallyRepaired())
            {
                calculatedRepairedAt = partiallyRepairedTableRepairedAt(repairedAt.getMaxRepairedAt());
            }
            else if(myRepairConfiguration.getRepairIntervalInMs() != 0)
            {
                calculatedRepairedAt = newTableRepairedAt();
            }
        }
        else
        {
            calculatedRepairedAt = repairedTableRepairedAt(repairedAt.getMinRepairedAt());
        }

        return calculatedRepairedAt;
    }

    private long repairedTableRepairedAt(long repairedAt)
    {
        if (LOG.isInfoEnabled() && myRepairConfiguration.getRepairIntervalInMs() != 0)
        {
            LOG.info("Table {} last repaired at {}, next repair {}", myTableReference, myDateFormat.get().format(new Date(repairedAt)), myDateFormat.get().format(new Date(repairedAt + myRepairConfiguration.getRepairIntervalInMs())));
        }
        return repairedAt;
    }

    private long partiallyRepairedTableRepairedAt(long maxRepairedAt)
    {
        long runIntervalInMs = myRepairConfiguration.getRepairIntervalInMs();
        long repairedAt = Math.min(System.currentTimeMillis() - runIntervalInMs, maxRepairedAt);
        if (LOG.isInfoEnabled() && myRepairConfiguration.getRepairIntervalInMs() != 0)
        {
            LOG.info("Table {} has been partially repaired, next repair {}", myTableReference, myDateFormat.get().format(new Date(repairedAt + runIntervalInMs)));
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
            LOG.info("Assuming the table {} is new, next repair {}", myTableReference, myDateFormat.get().format(new Date(assumedRepairedAt + runIntervalInMs)));
        }

        return assumedRepairedAt;
    }

    private boolean replicasAreRepairable(VnodeRepairState vnodeRepairState)
    {
        for (Node node : vnodeRepairState.getReplicas())
        {
            if (!myHostStates.isUp(node))
            {
                LOG.trace("{} not repairable, host {} is not up", vnodeRepairState, node);
                return false;
            }
        }
        return true;
    }

    private boolean vnodeIsRepairable(VnodeRepairState vnodeRepairState)
    {
        long repairedBefore = System.currentTimeMillis() - myRepairConfiguration.getRepairIntervalInMs();

        return vnodeRepairState.lastRepairedAt() <= repairedBefore;
    }
}
