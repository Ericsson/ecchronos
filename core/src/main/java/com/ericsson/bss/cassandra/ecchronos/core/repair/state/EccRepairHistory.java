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

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.Node;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;

public class EccRepairHistory implements RepairHistory, RepairHistoryProvider
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

    private final Session session;
    private final Node localNode;
    private final StatementDecorator statementDecorator;
    private final ReplicationState replicationState;

    private final PreparedStatement iterateStatement;

    private final PreparedStatement initiateStatement;
    private final PreparedStatement finishStatement;

    private EccRepairHistory(Builder builder)
    {
        Preconditions.checkArgument(builder.lookbackTimeInMs > 0,
                "Lookback time must be a positive number");

        session = Preconditions.checkNotNull(builder.session, "Session cannot be null");
        localNode = Preconditions.checkNotNull(builder.localNode, "Local node must be set");
        statementDecorator = Preconditions.checkNotNull(builder.statementDecorator, "Statement decorator must be set");
        replicationState = Preconditions.checkNotNull(builder.replicationState, "Replication state must be set");
        lookbackTimeInMs = builder.lookbackTimeInMs;

        initiateStatement = session.prepare(QueryBuilder.insertInto(builder.keyspaceName, "repair_history")
                .value(COLUMN_TABLE_ID, bindMarker())
                .value(COLUMN_NODE_ID, bindMarker())
                .value(COLUMN_REPAIR_ID, bindMarker())
                .value(COLUMN_JOB_ID, bindMarker())
                .value(COLUMN_COORDINATOR_ID, bindMarker())
                .value(COLUMN_RANGE_BEGIN, bindMarker())
                .value(COLUMN_RANGE_END, bindMarker())
                .value(COLUMN_STATUS, bindMarker())
                .value(COLUMN_STARTED_AT, bindMarker()))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        finishStatement = session.prepare(QueryBuilder.update(builder.keyspaceName, "repair_history")
                .with(set(COLUMN_STATUS, bindMarker()))
                .and(set(COLUMN_FINISHED_AT, bindMarker()))
                .where(eq(COLUMN_TABLE_ID, bindMarker()))
                .and(eq(COLUMN_NODE_ID, bindMarker()))
                .and(eq(COLUMN_REPAIR_ID, bindMarker())))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        iterateStatement = session.prepare(
                QueryBuilder.select(COLUMN_STARTED_AT, COLUMN_FINISHED_AT, COLUMN_STATUS, COLUMN_RANGE_BEGIN, COLUMN_RANGE_END)
                        .from(builder.keyspaceName, "repair_history")
                        .where(eq(COLUMN_TABLE_ID, bindMarker()))
                        .and(eq(COLUMN_NODE_ID, bindMarker()))
                        .and(gte(COLUMN_REPAIR_ID, bindMarker()))
                        .and(lte(COLUMN_REPAIR_ID, bindMarker())))
                .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
    }

    @Override
    public RepairSession newSession(TableReference tableReference, UUID jobId, LongTokenRange range,
            Set<Node> participants)
    {
        Preconditions.checkArgument(participants.contains(localNode),
                "Local node must be part of repair");
        ImmutableSet<Node> nodes = replicationState.getNodes(tableReference, range);
        if (nodes == null || !nodes.equals(participants))
        {
            return new NoOpRepairSession();
        }

        return new RepairSessionImpl(tableReference.getId(), localNode.getId(), jobId, range, participants);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate)
    {
        long from = System.currentTimeMillis() - lookbackTimeInMs;
        return iterate(tableReference, to, from, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from,
            Predicate<RepairEntry> predicate)
    {
        UUID start = UUIDs.startOf(from);
        UUID finish = UUIDs.endOf(to);

        Statement statement = iterateStatement.bind(tableReference.getId(), localNode.getId(), start, finish);
        ResultSet resultSet = execute(statement);

        return new RepairEntryIterator(tableReference, resultSet, predicate);
    }

    private ResultSet execute(Statement statement)
    {
        return session.execute(statementDecorator.apply(statement));
    }

    private ResultSetFuture executeAsync(Statement statement)
    {
        return session.executeAsync(statementDecorator.apply(statement));
    }

    class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final TableReference tableReference;
        private final Iterator<Row> rowIterator;
        private final Predicate<RepairEntry> predicate;

        RepairEntryIterator(TableReference tableReference, ResultSet resultSet, Predicate<RepairEntry> predicate)
        {
            this.tableReference = tableReference;
            this.rowIterator = resultSet.iterator();
            this.predicate = predicate;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while (rowIterator.hasNext())
            {
                Row row = rowIterator.next();

                if (validateFields(row))
                {
                    RepairEntry repairEntry = buildFrom(row);
                    if (repairEntry != null && predicate.apply(repairEntry))
                    {
                        return repairEntry;
                    }
                }
            }

            return endOfData();
        }

        private RepairEntry buildFrom(Row row)
        {
            long rangeBegin = Long.parseLong(row.getString(COLUMN_RANGE_BEGIN));
            long rangeEnd = Long.parseLong(row.getString(COLUMN_RANGE_END));

            LongTokenRange tokenRange = new LongTokenRange(rangeBegin, rangeEnd);
            long startedAt = row.getTimestamp(COLUMN_STARTED_AT).getTime();
            Date finished = row.getTimestamp(COLUMN_FINISHED_AT);
            long finishedAt = -1L;
            if(finished != null)
            {
                finishedAt = finished.getTime();
            }
            Set<Node> nodes = replicationState.getNodes(tableReference, tokenRange);
            if (nodes == null)
            {
                LOG.debug("Token range {} was not found in metadata", tokenRange);
                return null;
            }
            String status = row.getString(COLUMN_STATUS);

            return new RepairEntry(tokenRange, startedAt, finishedAt, nodes, status);
        }

        private boolean validateFields(Row row)
        {
            return !row.isNull(COLUMN_RANGE_BEGIN) &&
                    !row.isNull(COLUMN_RANGE_END) &&
                    !row.isNull(COLUMN_STARTED_AT) &&
                    !row.isNull(COLUMN_STATUS);
        }
    }

    private enum SessionState
    {
        DONE(null), STARTED(DONE), NO_STATE(STARTED);

        private final SessionState nextValid;

        SessionState(SessionState nextValid)
        {
            this.nextValid = nextValid;
        }

        public boolean canTransition(SessionState nextState)
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

        RepairSessionImpl(UUID tableId, UUID nodeId, UUID jobId, LongTokenRange range, Set<Node> participants)
        {
            this.tableId = tableId;
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.range = range;
            this.participants = participants.stream()
                    .map(Node::getId)
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
            repairId.compareAndSet(null, UUIDs.timeBased());
            transitionTo(SessionState.STARTED);
            String range_begin = Long.toString(range.start);
            String range_end = Long.toString(range.end);
            Date started_at = new Date(UUIDs.unixTimestamp(repairId.get()));

            insertWithRetry(participant -> insertStart(range_begin, range_end, started_at, participant));
        }

        @Override
        public void finish(RepairStatus repairStatus)
        {
            Preconditions.checkArgument(!RepairStatus.STARTED.equals(repairStatus),
                    "Repair status must change from started");
            transitionTo(SessionState.DONE);
            Date finished_at = new Date(System.currentTimeMillis());

            insertWithRetry(participant -> insertFinish(repairStatus, finished_at, participant));
        }

        private void insertWithRetry(Function<UUID, ResultSetFuture> insertFunction)
        {
            Map<UUID, ResultSetFuture> futures = new HashMap<>();

            for (UUID participant : participants)
            {
                ResultSetFuture future = insertFunction.apply(participant);
                futures.put(participant, future);
            }

            boolean loggedException = false;

            for (Map.Entry<UUID, ResultSetFuture> entry : futures.entrySet())
            {
                ResultSetFuture future = entry.getValue();

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

        private ResultSetFuture insertStart(String range_begin, String range_end, Date started_at, UUID participant)
        {
            Statement statement = initiateStatement.bind(tableId, participant, repairId.get(), jobId, nodeId, range_begin,
                    range_end, RepairStatus.STARTED.toString(), started_at);
            return executeAsync(statement);
        }

        private ResultSetFuture insertFinish(RepairStatus repairStatus, Date finished_at, UUID participant)
        {
            Statement statement = finishStatement.bind(repairStatus.toString(), finished_at, tableId, participant,
                    repairId.get());
            return executeAsync(statement);
        }

        @Override
        public String toString()
        {
            return String.format("table_id=%s,repair_id=%s,job_id=%s,range=%s,participants=%s", tableId, repairId.get(),
                    jobId, range, participants);
        }

        private void transitionTo(SessionState newState)
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
        private Session session;
        private Node localNode;
        private StatementDecorator statementDecorator;
        private ReplicationState replicationState;
        private long lookbackTimeInMs;
        private String keyspaceName = "ecchronos";

        public Builder withSession(Session session)
        {
            this.session = session;
            return this;
        }

        public Builder withLocalNode(Node localNode)
        {
            this.localNode = localNode;
            return this;
        }

        public Builder withStatementDecorator(StatementDecorator statementDecorator)
        {
            this.statementDecorator = statementDecorator;
            return this;
        }

        public Builder withReplicationState(ReplicationState replicationState)
        {
            this.replicationState = replicationState;
            return this;
        }

        public Builder withLookbackTime(long lookbackTime, TimeUnit unit)
        {
            this.lookbackTimeInMs = TimeUnit.MILLISECONDS.convert(lookbackTime, unit);
            return this;
        }

        public Builder withKeyspace(String keyspaceName)
        {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public EccRepairHistory build()
        {
            return new EccRepairHistory(this);
        }
    }
}
