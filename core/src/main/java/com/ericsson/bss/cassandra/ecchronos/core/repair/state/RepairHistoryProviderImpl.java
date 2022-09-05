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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.utils.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Implementation of the RepairHistoryProvider interface that retrieves the repair history from Cassandra.
 */
public class RepairHistoryProviderImpl implements RepairHistoryProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairHistoryProviderImpl.class);

    private static final String RANGE_BEGIN_COLUMN = "range_begin";
    private static final String RANGE_END_COLUMN = "range_end";
    private static final String STATUS_COLUMN = "status";
    private static final String PARTICIPANTS_COLUMN = "participants";
    private static final String COORDINATOR_COLUMN = "coordinator";
    private static final String STARTED_AT_COLUMN = "started_at";
    private static final String FINISHED_AT_COLUMN = "finished_at";

    private static final String KEYSPACE_NAME = "system_distributed";
    private static final String REPAIR_HISTORY = "repair_history";

    private static final String REPAIR_HISTORY_BY_TIME_STATEMENT = String
            .format("SELECT started_at, finished_at, range_begin, range_end, status, participants, coordinator FROM %s.%s WHERE keyspace_name=? AND columnfamily_name=? AND id >= minTimeuuid(?) and id <= maxTimeuuid(?)",
                    KEYSPACE_NAME, REPAIR_HISTORY);

    private final NodeResolver myNodeResolver;
    private final CqlSession mySession;
    private final StatementDecorator myStatementDecorator;

    private final PreparedStatement myRepairHistoryByTimeStatement;
    private final long myLookbackTime;
    private final Clock myClock;

    public RepairHistoryProviderImpl(NodeResolver nodeResolver, CqlSession session,
            StatementDecorator statementDecorator,
            long lookbackTime)
    {
        this(nodeResolver, session, statementDecorator, lookbackTime, Clock.systemDefaultZone());
    }

    @VisibleForTesting
    RepairHistoryProviderImpl(NodeResolver nodeResolver, CqlSession session,
                              StatementDecorator statementDecorator,
                              long lookbackTime, Clock clock)
    {
        myNodeResolver = nodeResolver;
        mySession = session;
        myStatementDecorator = statementDecorator;
        myRepairHistoryByTimeStatement = mySession.prepare(REPAIR_HISTORY_BY_TIME_STATEMENT);
        myLookbackTime = lookbackTime;
        myClock = clock;
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate)
    {
        long from = myClock.millis() - myLookbackTime;
        return iterate(tableReference, to, from, predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from,
            Predicate<RepairEntry> predicate)
    {
        Instant fromDate = Instant.ofEpochMilli(from);
        Instant toDate = Instant.ofEpochMilli(to);
        if (!fromDate.isBefore(toDate))
        {
            throw new IllegalArgumentException(
                    "Invalid range when iterating " + tableReference + ", from (" + fromDate + ") to (" + toDate + ")");
        }
        ResultSet resultSet =
                execute(myRepairHistoryByTimeStatement.bind(tableReference.getKeyspace(), tableReference.getTable(),
                        fromDate, toDate));

        return new RepairEntryIterator(resultSet.iterator(), predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(UUID nodeId, TableReference tableReference, long to, long from,
            Predicate<RepairEntry> predicate)
    {
        return iterate(tableReference, to, from, predicate);
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(myStatementDecorator.apply(statement));
    }

    class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final Iterator<Row> myIterator;
        private final Predicate<RepairEntry> myPredicate;

        RepairEntryIterator(Iterator<Row> iterator, Predicate<RepairEntry> predicate)
        {
            myIterator = iterator;
            myPredicate = predicate;
        }

        @Override
        protected RepairEntry computeNext()
        {
            while (myIterator.hasNext())
            {
                Row row = myIterator.next();

                if (validateFields(row))
                {
                    long rangeBegin = Long.parseLong(row.getString(RANGE_BEGIN_COLUMN));
                    long rangeEnd = Long.parseLong(row.getString(RANGE_END_COLUMN));

                    LongTokenRange tokenRange = new LongTokenRange(rangeBegin, rangeEnd);
                    Set<InetAddress> participants = row.getSet(PARTICIPANTS_COLUMN, InetAddress.class);
                    Set<DriverNode> nodes = new HashSet<>();
                    InetAddress coordinator = row.get(COORDINATOR_COLUMN, InetAddress.class);
                    Optional<DriverNode> coordinatorNode = myNodeResolver.fromIp(coordinator);
                    if (!coordinatorNode.isPresent())
                    {
                        LOG.warn("Coordinator node {} not found in metadata", coordinator);
                    }
                    else
                    {
                        nodes.add(coordinatorNode.get());
                    }
                    for (InetAddress participant : participants)
                    {
                        Optional<DriverNode> node = myNodeResolver.fromIp(participant);
                        if (!node.isPresent())
                        {
                            LOG.warn("Node {} not found in metadata", participant);
                        }
                        else
                        {
                            nodes.add(node.get());
                        }
                    }
                    String status = row.getString(STATUS_COLUMN);
                    long startedAt = row.getInstant(STARTED_AT_COLUMN).toEpochMilli();
                    Instant finished = row.getInstant(FINISHED_AT_COLUMN);
                    long finishedAt = -1L;
                    if (finished != null)
                    {
                        finishedAt = finished.toEpochMilli();
                    }

                    RepairEntry repairEntry = new RepairEntry(tokenRange, startedAt, finishedAt, nodes, status);

                    if (myPredicate.apply(repairEntry))
                    {
                        return repairEntry;
                    }
                }
            }

            return endOfData();
        }

        private boolean validateFields(Row row)
        {
            return !row.isNull(PARTICIPANTS_COLUMN) &&
                    !row.isNull(RANGE_BEGIN_COLUMN) &&
                    !row.isNull(RANGE_END_COLUMN) &&
                    !row.isNull(COORDINATOR_COLUMN) &&
                    !row.isNull(STARTED_AT_COLUMN);
        }
    }
}
