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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.ericsson.bss.cassandra.ecchronos.core.Clock;
import com.ericsson.bss.cassandra.ecchronos.core.HostStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.metrics.TableRepairMetrics;
import com.ericsson.bss.cassandra.ecchronos.core.utils.KeyspaceHelper;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;

/**
 * Implementation of the repair state that retrieves information from the repair history provider
 * to keep track of what repair state a specific table has.
 * <p>
 * The repair state has a number of sub-states that includes:
 * <ul>
 * <li>Unknown</li>
 * <li>Repaired</li>
 * <li>Not repaired and runnable</li>
 * <li>Not repaired and not runnable</li>
 * </ul>
 */
public class RepairStateImpl implements RepairState
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairStateImpl.class);

    private final SimpleDateFormat myDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    private static final long MAX_WAIT_BETWEEN_NODES_IN_MS = TimeUnit.HOURS.toMillis(1);
    private static final int MIN_REPLICAS_FOR_REPAIR = 2;

    private final TableReference myTableReference;
    private final Host myHost;
    private final Metadata myMetadata;
    private final HostStates myHostStates;
    private final RepairHistoryProvider myRepairHistoryProvider;
    private final long myRunIntervalInMs;
    private final TableRepairMetrics myTableRepairMetrics;

    private final AtomicReference<State> myState = new AtomicReference<>();

    private final AtomicReference<Clock> myClock = new AtomicReference<>(Clock.DEFAULT);

    /**
     * Abstract class for the different sub states.
     */
    abstract class State implements RepairState
    {
        private final long myLastRepairedAt;

        private State(long lastRepairedAt)
        {
            myLastRepairedAt = lastRepairedAt;
        }

        @Override
        public RepairStateSnapshot getSnapshot()
        {
            return RepairStateSnapshot.newBuilder()
                    .withLastRepairedAt(lastRepairedAt())
                    .canRepair(canRepair())
                    .withDataCenters(getDatacentersForRepair())
                    .withLocalRangesForRepair(getLocalRangesForRepair())
                    .withRanges(getAllRanges())
                    .withReplicas(getReplicas())
                    .withRangeToReplica(getRangeToReplicas())
                    .build();
        }

        public abstract boolean canRepair();

        public final long lastRepairedAt()
        {
            return myLastRepairedAt;
        }

        public void update() // NOPMD
        {
            // Not used by sub-states
        }

        public Collection<LongTokenRange> getAllRanges()
        {
            return getLocalRanges();
        }

        public Collection<LongTokenRange> getLocalRangesForRepair()
        {
            return Sets.newHashSet();
        }

        public Set<Host> getReplicas()
        {
            return Sets.newHashSet();
        }

        public Collection<String> getDatacentersForRepair()
        {
            Collection<String> dataCenters = new HashSet<>();
            if (canRepair())
            {
                Collection<Host> replicas = getReplicas();

                if (!replicas.isEmpty())
                {
                    for (Host replica : replicas)
                    {
                        if (myHostStates.isUp(replica))
                        {
                            dataCenters.add(replica.getDatacenter());
                        }
                    }
                }
                else
                {
                    dataCenters = KeyspaceHelper.getDatacentersForKeyspace(myMetadata, myTableReference.getKeyspace());
                }
            }

            return dataCenters;
        }

        public Map<LongTokenRange, Collection<Host>> getRangeToReplicas()
        {
            Map<LongTokenRange, Collection<Host>> rangeToReplicas = getAllRangeToReplicas();
            removeRepairedRanges(rangeToReplicas);
            removeUnavailableReplicas(rangeToReplicas);

            return rangeToReplicas;
        }

        private void removeRepairedRanges(Map<LongTokenRange, Collection<Host>> rangeToReplicas)
        {
            Collection<LongTokenRange> rangesToRepair = getLocalRangesForRepair();

            if (!rangesToRepair.equals(getLocalRanges()))
            {
                Iterator<Map.Entry<LongTokenRange, Collection<Host>>> iterator = rangeToReplicas.entrySet().iterator();

                while (iterator.hasNext())
                {
                    Map.Entry<LongTokenRange, Collection<Host>> entry = iterator.next();

                    if (!rangesToRepair.contains(entry.getKey()))
                    {
                        LOG.debug("Range {} already repaired, not included in current job", entry.getKey());
                        iterator.remove();
                    }
                }
            }
        }

        private void removeUnavailableReplicas(Map<LongTokenRange, Collection<Host>> rangeToReplicas)
        {
            Set<Host> replicas = getReplicas();

            if (!replicas.isEmpty())
            {
                Map<LongTokenRange, Collection<Host>> reducedRanges = new HashMap<>();

                Set<Host> unavailableReplicas = Sets.difference(getReplicasForRepair(), replicas);

                Iterator<Map.Entry<LongTokenRange, Collection<Host>>> iterator = rangeToReplicas.entrySet().iterator();

                while (iterator.hasNext())
                {
                    Map.Entry<LongTokenRange, Collection<Host>> entry = iterator.next();

                    boolean rangeReduced = entry.getValue().removeAll(unavailableReplicas);

                    if (entry.getValue().size() < MIN_REPLICAS_FOR_REPAIR)
                    {
                        LOG.warn("Range {} does not have enough replicas available for repair", entry.getKey());
                        iterator.remove();
                    }
                    else if (rangeReduced)
                    {
                        reducedRanges.put(entry.getKey(), entry.getValue());
                    }
                }

                LOG.warn("Running repair in reduced mode for replicas {}, ranges {}", unavailableReplicas, reducedRanges.keySet());
            }
        }

        public abstract State next();
    }

    private RepairStateImpl(Builder builder)
    {
        myTableReference = builder.myTableReference;
        myMetadata = builder.myMetadata;
        myHost = builder.myHost;
        myHostStates = builder.myHostStates;
        myRepairHistoryProvider = builder.myRepairHistoryProvider;
        myRunIntervalInMs = builder.myRunIntervalInMs;
        myTableRepairMetrics = builder.myTableRepairMetrics;

        myState.set(new UnknownState(-1));
    }

    @Override
    public RepairStateSnapshot getSnapshot()
    {
        return myState.get().getSnapshot();
    }

    @Override
    public void update()
    {
        State newState = myState.get().next();
        State previous = myState.getAndSet(newState);

        if (!previous.equals(newState))
        {
            LOG.debug("Changed repair state from {} -> {} for {} - last repaired {} -> {}", previous, newState, myTableReference, previous.lastRepairedAt(), newState.lastRepairedAt());

            myTableRepairMetrics.lastRepairedAt(myTableReference, newState.lastRepairedAt());

            Set<LongTokenRange> allRanges = Sets.newHashSet(newState.getAllRanges());
            Set<LongTokenRange> notRepairedRanges = Sets.newHashSet(newState.getLocalRangesForRepair());
            Set<LongTokenRange> repairedRanges = Sets.difference(allRanges, notRepairedRanges);

            myTableRepairMetrics.repairState(myTableReference, repairedRanges.size(), notRepairedRanges.size());
        }
        else
        {
            LOG.debug("Keeping state of {} for {}", previous, myTableReference);
        }
    }

    @VisibleForTesting
    boolean canRepair()
    {
        return myState.get().canRepair();
    }

    @VisibleForTesting
    long lastRepairedAt()
    {
        return myState.get().lastRepairedAt();
    }

    @VisibleForTesting
    Set<Host> getReplicas()
    {
        return myState.get().getReplicas();
    }

    @VisibleForTesting
    Collection<LongTokenRange> getLocalRangesForRepair()
    {
        return myState.get().getLocalRangesForRepair();
    }

    @VisibleForTesting
    Collection<LongTokenRange> getAllRanges()
    {
        return myState.get().getAllRanges();
    }

    @VisibleForTesting
    Map<LongTokenRange, Collection<Host>> getRangeToReplicas()
    {
        return myState.get().getRangeToReplicas();
    }

    @VisibleForTesting
    Collection<String> getDatacentersForRepair()
    {
        return myState.get().getDatacentersForRepair();
    }

    @VisibleForTesting
    State getState()
    {
        return myState.get();
    }

    @VisibleForTesting
    void setClock(Clock clock)
    {
        myClock.set(clock);
    }

    private State nextState()
    {
        long now = myClock.get().getTime();
        long previousLastRepairedAt = myState.get().lastRepairedAt();

        Iterator<RepairEntry> iterator = myRepairHistoryProvider.iterate(myTableReference, now, previousLastRepairedAt, new FullyRepairedRepairEntryPredicate(getAllRangeToReplicas()));

        Map<LongTokenRange, Long> repairedAt = getLastRepairedAt(iterator);

        long lastRepairedAt = calculateEarliestRepairedAt(repairedAt);

        lastRepairedAt = lastRepairedAt == -1 ? previousLastRepairedAt : lastRepairedAt;

        Set<LongTokenRange> nonRepairedRanges = getNotRepairedRanges(repairedAt);

        if (!nonRepairedRanges.isEmpty())
        {
            return getRepairState(lastRepairedAt, nonRepairedRanges);
        }
        else
        {
            if (LOG.isDebugEnabled())
            {
                LOG.debug("Table {} last repaired at {}, next repair {}", myTableReference, myDateFormat.format(new Date(lastRepairedAt)), myDateFormat.format(new Date(lastRepairedAt + myRunIntervalInMs)));
            }
            return new RepairedState(lastRepairedAt);
        }
    }

    private State getRepairState(long lastRepairedAt, Set<LongTokenRange> nonRepairedRanges)
    {
        State newState = null;

        if (isPartialRepairRequired() && allowPartialRepair(lastRepairedAt) && validateNotRepairedRecently())
        {
            Set<Host> hosts = hostsForPartialRepair();

            if (hosts.size() >= MIN_REPLICAS_FOR_REPAIR)
            {
                newState = new NotRepairedRunnableState(lastRepairedAt, hosts, nonRepairedRanges);
            }
        }
        else if (validateEnoughLiveHosts() && validateNotRepairedRecently())
        {
            newState = new NotRepairedRunnableState(lastRepairedAt, nonRepairedRanges);
        }

        if (newState == null)
        {
            newState = new NotRepairedNotRunnableState(lastRepairedAt, nonRepairedRanges);
        }

        return newState;
    }

    private Set<LongTokenRange> getNotRepairedRanges(Map<LongTokenRange, Long> lastRepairedAt)
    {
        Set<LongTokenRange> repairedRanges = calculateRepairedRanges(lastRepairedAt);

        return Sets.difference(getLocalRanges(), repairedRanges);
    }

    private Map<LongTokenRange, Long> getLastRepairedAt(Iterator<RepairEntry> iterator)
    {
        Map<LongTokenRange, Long> repairedAt = new HashMap<>();

        for (LongTokenRange tokenRange : getLocalRanges())
        {
            repairedAt.put(tokenRange, -1L);
        }

        while (iterator.hasNext())
        {
            RepairEntry repairEntry = iterator.next();

            Long previousRepairedAt = repairedAt.get(repairEntry.getRange());

            if (previousRepairedAt != null && (previousRepairedAt == -1 || previousRepairedAt < repairEntry.getStartedAt()))
            {
                repairedAt.put(repairEntry.getRange(), repairEntry.getStartedAt());
            }
        }

        return repairedAt;
    }

    private Set<LongTokenRange> calculateRepairedRanges(Map<LongTokenRange, Long> repairedAt)
    {
        long now = myClock.get().getTime();

        Set<LongTokenRange> repairedRanges = new HashSet<>();

        for (Map.Entry<LongTokenRange, Long> repaired : repairedAt.entrySet())
        {
            long lastRepairedAt = repaired.getValue();
            if (lastRepairedAt > now - myRunIntervalInMs)
            {
                repairedRanges.add(repaired.getKey());
            }
        }

        return repairedRanges;
    }

    private long calculateEarliestRepairedAt(Map<LongTokenRange, Long> repairedAt)
    {
        if (repairedAt.isEmpty())
        {
            return -1L;
        }

        long lastRepairedAt = Long.MAX_VALUE;

        for (Long repaired : repairedAt.values())
        {
            if (repaired < lastRepairedAt)
            {
                lastRepairedAt = repaired;
            }
        }

        return lastRepairedAt;
    }

    private boolean validateNotRepairedRecently()
    {
        Set<Host> allHosts = myMetadata.getAllHosts();

        long wantedWaitBetweenNodes = myRunIntervalInMs / allHosts.size();
        long actualWaitBetweenNodes = Math.min(wantedWaitBetweenNodes, MAX_WAIT_BETWEEN_NODES_IN_MS);

        long to = myClock.get().getTime();
        long from = to - actualWaitBetweenNodes;

        Iterator<RepairEntry> iterator = myRepairHistoryProvider.iterate(myTableReference, to, from, Predicates.alwaysTrue());

        return !iterator.hasNext();
    }

    private boolean validateEnoughLiveHosts()
    {
        Set<Host> repairReplicas = getReplicasForRepair();
        LOG.trace("Replicas for repair: {}", repairReplicas);
        for (Host host : repairReplicas)
        {
            if (!myHostStates.isUp(host))
            {
                LOG.debug("Host {} not up, not starting repair", host);
                return false;
            }
        }

        return true;
    }

    private Set<Host> hostsForPartialRepair()
    {
        Set<Host> hosts = new HashSet<>();

        Collection<TokenRange> ranges = getLocalTokens();

        for (TokenRange range : ranges)
        {
            Set<Host> replicasForRange = myMetadata.getReplicas(myTableReference.getKeyspace(), range);

            for (Host host : replicasForRange)
            {
                if (myHostStates.isUp(host))
                {
                    hosts.add(host);
                }
            }
        }

        return hosts;
    }

    private boolean isPartialRepairRequired()
    {
        Collection<TokenRange> ranges = getLocalTokens();

        for (TokenRange range : ranges)
        {
            Set<Host> replicasForRange = myMetadata.getReplicas(myTableReference.getKeyspace(), range);

            for (Host host : replicasForRange)
            {
                if (!myHostStates.isUp(host))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean allowPartialRepair(long lastRepairedAt)
    {
        return lastRepairedAt != -1 && lastRepairedAt <= myClock.get().getTime() - (myRunIntervalInMs * 2);
    }

    private Set<LongTokenRange> getLocalRanges()
    {
        Set<LongTokenRange> ranges = new HashSet<>();

        for (TokenRange range : getLocalTokens())
        {
            ranges.add(convert(range));
        }

        return ranges;
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }

    private Collection<TokenRange> getLocalTokens()
    {
        return myMetadata.getTokenRanges(myTableReference.getKeyspace(), myHost);
    }

    private Set<Host> getReplicasForRepair()
    {
        Collection<TokenRange> ranges = getLocalTokens();
        Set<Host> hosts = new HashSet<>();

        for (TokenRange range : ranges)
        {
            hosts.addAll(myMetadata.getReplicas(myTableReference.getKeyspace(), range));
        }

        return hosts;
    }

    Map<LongTokenRange, Collection<Host>> getAllRangeToReplicas()
    {
        Map<LongTokenRange, Collection<Host>> rangesToReplicas = new HashMap<>();

        for (TokenRange range : myMetadata.getTokenRanges(myTableReference.getKeyspace(), myHost))
        {
            rangesToReplicas.put(convert(range), Sets.newHashSet(myMetadata.getReplicas(myTableReference.getKeyspace(), range)));
        }

        return rangesToReplicas;
    }

    @Override
    public String toString()
    {
        return String.format("Repair state of %s on %s", myTableReference, myHost);
    }

    /**
     * The unknown state is the basic state which is used when the repair state is constructed.
     */
    class UnknownState extends State
    {
        UnknownState(long lastRepairedAt)
        {
            super(lastRepairedAt);
        }

        @Override
        public boolean canRepair()
        {
            return false;
        }

        @Override
        public State next()
        {
            long now = myClock.get().getTime();

            Iterator<RepairEntry> iterator = myRepairHistoryProvider.iterate(myTableReference, now, new FullyRepairedRepairEntryPredicate(getAllRangeToReplicas()));

            Map<LongTokenRange, Long> repairedAt = getLastRepairedAt(iterator);

            long lastRepairedAt = calculateEarliestRepairedAt(repairedAt);

            if (lastRepairedAt == -1)
            {
                long minimumRepairWait = Math.min(myRunIntervalInMs, TimeUnit.DAYS.toMillis(1));
                long assumedPreviousRepair = now - myRunIntervalInMs + minimumRepairWait;

                if (LOG.isDebugEnabled())
                {
                    LOG.debug("Assuming the table {} is new, next repair {}", myTableReference, myDateFormat.format(new Date(assumedPreviousRepair + myRunIntervalInMs)));
                }
                return new RepairedState(assumedPreviousRepair);
            }
            else
            {
                Set<LongTokenRange> nonRepairedRanges = getNotRepairedRanges(repairedAt);

                if (!nonRepairedRanges.isEmpty())
                {
                    LOG.debug("Number of not repaired ranges for table {}: {}", myTableReference, nonRepairedRanges.size());
                    return getRepairState(lastRepairedAt, nonRepairedRanges);
                }
                else
                {
                    if (LOG.isDebugEnabled())
                    {
                        LOG.debug("Table {} last repaired at {}, next repair {}", myTableReference, myDateFormat.format(new Date(lastRepairedAt)), myDateFormat.format(new Date(lastRepairedAt + myRunIntervalInMs)));
                    }
                    return new RepairedState(lastRepairedAt);
                }
            }
        }

        @Override
        public String toString()
        {
            return "State{Unknown}";
        }
    }

    /**
     * The repaired state is used if the repair already has run within the interval window.
     */
    class RepairedState extends State
    {
        RepairedState(long lastRepairedAt)
        {
            super(lastRepairedAt);
        }

        @Override
        public State next()
        {
            if (!canRepair())
            {
                return this;
            }
            else
            {
                return nextState();
            }
        }

        @Override
        public boolean canRepair()
        {
            return super.lastRepairedAt() < myClock.get().getTime() - myRunIntervalInMs;
        }

        @Override
        public String toString()
        {
            return "State{Repaired}";
        }
    }

    /**
     * The not repaired and runnable state is used when repair has not run within the interval
     * but can be run (all required hosts are available).
     */
    class NotRepairedRunnableState extends State
    {
        private final Set<Host> myHosts;
        private final Set<LongTokenRange> myRanges;

        NotRepairedRunnableState(long lastRepairedAt, Set<LongTokenRange> ranges)
        {
            this(lastRepairedAt, Sets.newHashSet(), ranges);
        }

        NotRepairedRunnableState(long lastRepairedAt, Set<Host> hosts, Set<LongTokenRange> ranges)
        {
            super(lastRepairedAt);
            myHosts = Sets.newHashSet(hosts);
            myRanges = Sets.newHashSet(ranges);
        }

        @Override
        public State next()
        {
            return nextState();
        }

        @Override
        public boolean canRepair()
        {
            return true;
        }

        @Override
        public Set<Host> getReplicas()
        {
            return Sets.newHashSet(myHosts);
        }

        @Override
        public Collection<LongTokenRange> getLocalRangesForRepair()
        {
            return Sets.newHashSet(myRanges);
        }

        @Override
        public String toString()
        {
            return "State{Not repaired - Runnable}";
        }
    }

    /**
     * The not repaired and not runnable state is used when repair has not run within the interval
     * but cannot be run due to missing constraints such as all required hosts not being available.
     */
    class NotRepairedNotRunnableState extends State
    {
        private final Set<LongTokenRange> myRanges;

        NotRepairedNotRunnableState(long lastRepairedAt, Set<LongTokenRange> ranges)
        {
            super(lastRepairedAt);
            myRanges = Sets.newHashSet(ranges);
        }

        @Override
        public State next()
        {
            return nextState();
        }

        @Override
        public boolean canRepair()
        {
            return false;
        }

        @Override
        public Collection<LongTokenRange> getLocalRangesForRepair()
        {
            return Sets.newHashSet(myRanges);
        }

        @Override
        public String toString()
        {
            return "State{Not repaired - Not runnable}";
        }
    }

    public static final class Builder
    {
        private TableReference myTableReference;
        private Host myHost;
        private Metadata myMetadata;
        private HostStates myHostStates;
        private RepairHistoryProvider myRepairHistoryProvider;
        private long myRunIntervalInMs;
        private TableRepairMetrics myTableRepairMetrics = null;

        public Builder withTableReference(TableReference tableReference)
        {
            myTableReference = tableReference;
            return this;
        }

        public Builder withHost(Host host)
        {
            myHost = host;
            return this;
        }

        public Builder withMetadata(Metadata metadata)
        {
            myMetadata = metadata;
            return this;
        }

        public Builder withHostStates(HostStates hostStates)
        {
            myHostStates = hostStates;
            return this;
        }

        public Builder withRepairHistoryProvider(RepairHistoryProvider repairHistoryProvider)
        {
            myRepairHistoryProvider = repairHistoryProvider;
            return this;
        }

        public Builder withRunInterval(long runInterval, TimeUnit timeUnit)
        {
            myRunIntervalInMs = timeUnit.toMillis(runInterval);
            return this;
        }

        public Builder withRepairMetrics(TableRepairMetrics tableRepairMetrics)
        {
            myTableRepairMetrics = tableRepairMetrics;
            return this;
        }

        public RepairStateImpl build()
        {
            if (myTableRepairMetrics == null)
            {
                throw new IllegalStateException("Metric interface not set");
            }

            return new RepairStateImpl(this);
        }
    }
}
