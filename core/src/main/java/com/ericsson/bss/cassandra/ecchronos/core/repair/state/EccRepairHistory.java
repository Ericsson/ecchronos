/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

public final class EccRepairHistory implements RepairHistory, RepairHistoryProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(EccRepairHistory.class);

    private static final String COLUMN_TABLE_ID = "table_id";
    private static final String COLUMN_NODE_ID = "node_id";
    private static final String COLUMN_REPAIR_ID = "repair_id";
    private static final String COLUMN_JOB_ID = "job_id";
    private static final String COLUMN_COORDINATOR_ID = "coordinator_id";
    private static final String COLUMN_RANGE_BEGIN = "range_begin";
    private static final String COLUMN_RANGE_END = "range_end";
    private static final String COLUMN_STATUS = "status";
    private static final String COLUMN_STARTED_AT = "started_at";
    private static final String COLUMN_FINISHED_AT = "finished_at";

    private final long lookbackTimeInMs;

    private final CqlSession session;
    private final DriverNode localNode;
    private final StatementDecorator statementDecorator;
    private final ReplicationState replicationState;

    private final PreparedStatement iterateStatement;

    private final PreparedStatement createStatement;

    private EccRepairHistory(final Builder builder)
    {
        Preconditions.checkArgument(builder.lookbackTimeInMs > 0,
                "Lookback time must be a positive number");

        session = Preconditions.checkNotNull(builder.session,
                "Session cannot be null");
        localNode = Preconditions.checkNotNull(builder.localNode,
                "Local node must be set");
        statementDecorator = Preconditions.checkNotNull(builder.statementDecorator,
                "Statement decorator must be set");
        replicationState = Preconditions.checkNotNull(builder.replicationState,
                "Replication state must be set");
        lookbackTimeInMs = builder.lookbackTimeInMs;

        createStatement = session.prepare(QueryBuilder.insertInto(builder.keyspaceName, "repair_history")
                        .value(COLUMN_TABLE_ID, bindMarker())
                        .value(COLUMN_NODE_ID, bindMarker())
                        .value(COLUMN_REPAIR_ID, bindMarker())
                        .value(COLUMN_JOB_ID, bindMarker())
                        .value(COLUMN_COORDINATOR_ID, bindMarker())
                        .value(COLUMN_RANGE_BEGIN, bindMarker())
                        .value(COLUMN_RANGE_END, bindMarker())
                        .value(COLUMN_STATUS, bindMarker())
                        .value(COLUMN_STARTED_AT, bindMarker())
                        .value(COLUMN_FINISHED_AT, bindMarker())
                .build().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));

        iterateStatement = session.prepare(QueryBuilder.selectFrom(builder.keyspaceName, "repair_history")
                .columns(COLUMN_STARTED_AT, COLUMN_FINISHED_AT, COLUMN_STATUS, COLUMN_RANGE_BEGIN, COLUMN_RANGE_END)
                .whereColumn(COLUMN_TABLE_ID).isEqualTo(bindMarker())
                .whereColumn(COLUMN_NODE_ID).isEqualTo(bindMarker())
                .whereColumn(COLUMN_REPAIR_ID).isGreaterThanOrEqualTo(bindMarker())
                .whereColumn(COLUMN_REPAIR_ID).isLessThanOrEqualTo(bindMarker()).build()
                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE));
    }

    @Override
    public RepairSession newSession(final TableReference tableReference,
                                    final UUID jobId,
                                    final LongTokenRange range,
                                    final Set<DriverNode> participants)
    {
        Preconditions.checkArgument(participants.contains(localNode),
                "Local node must be part of repair");
        ImmutableSet<DriverNode> nodes = replicationState.getNodes(tableReference, range);
        if (nodes == null || !nodes.equals(participants))
        {
            return new NoOpRepairSession();
        }

        return new RepairSessionImpl(tableReference.getId(), localNode.getId(), jobId, range, participants);
    }

    @Override
    public Iterator<RepairEntry> iterate(final TableReference tableReference,
                                         final long to,
                                         final Predicate<RepairEntry> predicate)
    {
        long from = System.currentTimeMillis() - lookbackTimeInMs;
        return iterate(tableReference, to, from, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(final TableReference tableReference,
                                         final long to,
                                         final long from,
                                         final Predicate<RepairEntry> predicate)
    {
        return iterate(localNode.getId(), tableReference, to, from, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(final UUID nodeId,
                                         final TableReference tableReference,
                                         final long to,
                                         final long from,
                                         final Predicate<RepairEntry> predicate)
    {
        UUID start = Uuids.startOf(from);
        UUID finish = Uuids.endOf(to);

        boolean clusterWide = false;
        if (!nodeId.equals(localNode.getId()))
        {
            clusterWide = true;
        }
        Statement statement = iterateStatement.bind(tableReference.getId(), nodeId, start, finish);
        ResultSet resultSet = execute(statement);

        return new RepairEntryIterator(tableReference, resultSet, predicate, clusterWide);
    }

    private ResultSet execute(final Statement statement)
    {
        return session.execute(statementDecorator.apply(statement));
    }

    private CompletionStage<AsyncResultSet> executeAsync(final Statement statement)
    {
        return session.executeAsync(statementDecorator.apply(statement));
    }

    class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final TableReference tableReference;
        private final Iterator<Row> rowIterator;
        private final Predicate<RepairEntry> predicate;
        private final boolean clusterWide;

        RepairEntryIterator(final TableReference aTableReference,
                            final ResultSet aResultSet,
                            final Predicate<RepairEntry> aPredicate,
                            final boolean isClusterWide)
        {
            this.tableReference = aTableReference;
            this.rowIterator = aResultSet.iterator();
            this.predicate = aPredicate;
            this.clusterWide = isClusterWide;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while (rowIterator.hasNext())
            {
                Row row = rowIterator.next();

                if (validateFields(row))
                {
                    RepairEntry repairEntry = buildFrom(row, clusterWide);
                    if (repairEntry != null && predicate.apply(repairEntry))
                    {
                        return repairEntry;
                    }
                }
            }

            return endOfData();
        }

        private RepairEntry buildFrom(final Row row, final boolean isClusterWide)
        {
            long rangeBegin = Long.parseLong(row.getString(COLUMN_RANGE_BEGIN));
            long rangeEnd = Long.parseLong(row.getString(COLUMN_RANGE_END));

            LongTokenRange tokenRange = new LongTokenRange(rangeBegin, rangeEnd);
            long startedAt = row.getInstant(COLUMN_STARTED_AT).toEpochMilli();
            Instant finished = row.getInstant(COLUMN_FINISHED_AT);
            long finishedAt = -1L;
            if (finished != null)
            {
                finishedAt = finished.toEpochMilli();
            }
            Set<DriverNode> nodes;
            if (isClusterWide)
            {
                nodes = replicationState.getNodesClusterWide(tableReference, tokenRange);
            }
            else
            {
                nodes = replicationState.getNodes(tableReference, tokenRange);
            }
            if (nodes == null)
            {
                LOG.debug("Token range {} was not found in metadata", tokenRange);
                return null;
            }
            String status = row.getString(COLUMN_STATUS);

            return new RepairEntry(tokenRange, startedAt, finishedAt, nodes, status);
        }

        private boolean validateFields(final Row row)
        {
            return !row.isNull(COLUMN_RANGE_BEGIN)
                    && !row.isNull(COLUMN_RANGE_END)
                    && !row.isNull(COLUMN_STARTED_AT)
                    && !row.isNull(COLUMN_STATUS);
        }
    }

    private enum SessionState
    {
        DONE(null), STARTED(DONE), NO_STATE(STARTED);

        private final SessionState nextValid;

        SessionState(final SessionState theNextValid)
        {
            this.nextValid = theNextValid;
        }

        public boolean canTransition(final SessionState nextState)
        {
            return nextState.equals(nextValid);
        }
    }

    class RepairSessionImpl implements RepairSession
    {
        private final UUID tableId;
        private final UUID nodeId;
        private final UUID jobId;
        private final LongTokenRange range;
        private final Set<UUID> participants;
        private final AtomicReference<SessionState> sessionState = new AtomicReference<>(SessionState.NO_STATE);
        private final AtomicReference<UUID> repairId = new AtomicReference<>(null);
        private final AtomicReference<Instant> startedAt = new AtomicReference<>(null);

        RepairSessionImpl(final UUID aTableId,
                          final UUID aNodeId,
                          final UUID aJobId,
                          final LongTokenRange aRange,
                          final Set<DriverNode> theParticipants)
        {
            this.tableId = aTableId;
            this.nodeId = aNodeId;
            this.jobId = aJobId;
            this.range = aRange;
            this.participants = theParticipants.stream()
                    .map(DriverNode::getId)
                    .collect(Collectors.toSet());
        }

        @VisibleForTesting
        UUID getId()
        {
            return repairId.get();
        }

        @Override
        public void start()
        {
            transitionTo(SessionState.STARTED);
            startedAt.compareAndSet(null, Instant.now());
        }

        /**
         * Transition to state DONE, as long as the previous status was STARTED. Set finished at to current timestamp.
         *
         * @param repairStatus The repair status
         */
        @Override
        public void finish(final RepairStatus repairStatus)
        {
            Preconditions.checkArgument(!RepairStatus.STARTED.equals(repairStatus),
                    "Repair status must change from started");
            transitionTo(SessionState.DONE);
            String rangeBegin = Long.toString(range.start);
            String rangeEnd = Long.toString(range.end);
            Instant finishedAt = Instant.now();
            repairId.compareAndSet(null, Uuids.timeBased());
            insertWithRetry(participant -> insertFinish(rangeBegin, rangeEnd, repairStatus, finishedAt, participant));
        }

        private void insertWithRetry(final Function<UUID, CompletionStage<AsyncResultSet>> insertFunction)
        {
            Map<UUID, CompletableFuture> futures = new HashMap<>();

            for (UUID participant : participants)
            {
                CompletableFuture future = insertFunction.apply(participant).toCompletableFuture();
                futures.put(participant, future);
            }

            boolean loggedException = false;

            for (Map.Entry<UUID, CompletableFuture> entry : futures.entrySet())
            {
                CompletableFuture future = entry.getValue();

                try
                {
                    future.get(2, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException | TimeoutException e)
                {
                    UUID participant = entry.getKey();
                    if (!loggedException)
                    {
                        LOG.warn("Unable to update repair history for {} - {}, retrying", participant, this, e);
                        loggedException = true;
                    }
                    else
                    {
                        LOG.warn("Unable to update repair history for {} - {}, retrying", participant, this);
                    }
                    insertFunction.apply(participant);
                }
            }
        }

        private CompletionStage<AsyncResultSet> insertFinish(final String rangeBegin,
                                                             final String rangeEnd,
                                                             final RepairStatus repairStatus,
                                                             final Instant finishedAt,
                                                             final UUID participant)
        {
            Statement statement = createStatement.bind(tableId, participant, repairId.get(), jobId, nodeId, rangeBegin,
                    rangeEnd, repairStatus.toString(), startedAt.get(), finishedAt);
            return executeAsync(statement);
        }

        /**
         * Return a string representation.
         *
         * @return String
         */
        @Override
        public String toString()
        {
            return String.format("table_id=%s,repair_id=%s,job_id=%s,range=%s,participants=%s", tableId, repairId.get(),
                    jobId, range, participants);
        }

        private void transitionTo(final SessionState newState)
        {
            SessionState currentState = sessionState.get();
            Preconditions.checkState(currentState.canTransition(newState),
                    "Cannot transition from " + currentState + " to " + newState);

            if (!sessionState.compareAndSet(currentState, newState))
            {
                throw new IllegalStateException("Cannot transition from " + sessionState.get() + " to " + newState);
            }
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private CqlSession session;
        private DriverNode localNode;
        private StatementDecorator statementDecorator;
        private ReplicationState replicationState;
        private long lookbackTimeInMs;
        private String keyspaceName = "ecchronos";

        /**
         * Build ECC repair history with session.
         *
         * @param aSession Session.q
         * @return Builder
         */
        public Builder withSession(final CqlSession aSession)
        {
            this.session = aSession;
            return this;
        }

        /**
         * Build ECC repair history with local node.
         *
         * @param aLocalNode The local node.
         * @return Builder
         */
        public Builder withLocalNode(final DriverNode aLocalNode)
        {
            this.localNode = aLocalNode;
            return this;
        }

        /**
         * Build ECC repair history with statement decorator.
         *
         * @param aStatementDecorator The statement decorator.
         * @return Builder
         */
        public Builder withStatementDecorator(final StatementDecorator aStatementDecorator)
        {
            this.statementDecorator = aStatementDecorator;
            return this;
        }

        /**
         * Build ECC repair history with replication history.
         *
         * @param aReplicationState Replication state.
         * @return Builder
         */
        public Builder withReplicationState(final ReplicationState aReplicationState)
        {
            this.replicationState = aReplicationState;
            return this;
        }

        /**
         * Build ECC repair history with lookback time.
         *
         * @param lookbackTime  Lookback time.
         * @param unit Time unit.
         * @return Builder
         */
        public Builder withLookbackTime(final long lookbackTime, final TimeUnit unit)
        {
            this.lookbackTimeInMs = TimeUnit.MILLISECONDS.convert(lookbackTime, unit);
            return this;
        }

        /**
         * Build ECC repair history with keyspace.
         *
         * @param theKeyspaceName Keyspace.
         * @return Builder
         */
        public Builder withKeyspace(final String theKeyspaceName)
        {
            this.keyspaceName = theKeyspaceName;
            return this;
        }

        /**
         * Build ECC repair history.
         *
         * @return Builder
         */
        public EccRepairHistory build()
        {
            return new EccRepairHistory(this);
        }
    }
}
