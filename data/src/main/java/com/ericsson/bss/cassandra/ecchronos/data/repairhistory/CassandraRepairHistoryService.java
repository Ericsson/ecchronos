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
package com.ericsson.bss.cassandra.ecchronos.data.repairhistory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.NodeResolver;
import com.ericsson.bss.cassandra.ecchronos.core.state.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairEntry;
import com.ericsson.bss.cassandra.ecchronos.core.state.RepairHistoryProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import java.net.InetAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the RepairHistoryProvider interface that retrieves the repair history from Cassandra.
 */
public class CassandraRepairHistoryService implements RepairHistoryProvider
{
    private static final Logger LOG = LoggerFactory.getLogger(CassandraRepairHistoryService.class);

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
            .format("SELECT started_at, finished_at, range_begin, range_end, status, participants, coordinator "
                    + "FROM %s.%s WHERE keyspace_name=? AND columnfamily_name=? AND id >= minTimeuuid(?) and id <= "
                    + "maxTimeuuid(?)", KEYSPACE_NAME, REPAIR_HISTORY);

    private final NodeResolver myNodeResolver;
    private final CqlSession mySession;

    private final PreparedStatement myRepairHistoryByTimeStatement;
    private final long myLookbackTime;
    private final Clock myClock;

    public CassandraRepairHistoryService(final NodeResolver nodeResolver,
                                         final CqlSession session,
                                         final long lookbackTime)
    {
        this(nodeResolver, session, lookbackTime, Clock.systemDefaultZone());
    }

    @VisibleForTesting
    CassandraRepairHistoryService(final NodeResolver nodeResolver,
                                  final CqlSession session,
                                  final long lookbackTime,
                                  final Clock clock)
    {
        myNodeResolver = nodeResolver;
        mySession = session;
        myRepairHistoryByTimeStatement = mySession.prepare(REPAIR_HISTORY_BY_TIME_STATEMENT);
        myLookbackTime = lookbackTime;
        myClock = clock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<RepairEntry> iterate(final Node node,
                                         final TableReference tableReference,
                                         final long to,
                                         final Predicate<RepairEntry> predicate)
    {
        long from = myClock.millis() - myLookbackTime;
        return iterate(node, tableReference, to, from, predicate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<RepairEntry> iterate(final Node node,
                                         final TableReference tableReference,
                                         final long to,
                                         final long from,
                                         final Predicate<RepairEntry> predicate)
    {
        Instant fromDate = Instant.ofEpochMilli(from);
        Instant toDate = Instant.ofEpochMilli(to);
        if (!fromDate.isBefore(toDate))
        {
            throw new IllegalArgumentException(
                    "Invalid range when iterating " + tableReference + ", from (" + fromDate + ") to (" + toDate + ")");
        }
        ResultSet resultSet =
                execute(myRepairHistoryByTimeStatement.bind(tableReference.getKeyspace(), tableReference.getTable(), fromDate, toDate));
        return new RepairEntryIterator(resultSet.iterator(), predicate);
    }

    private ResultSet execute(final Statement statement)
    {
        return mySession.execute(statement);
    }

    class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final Iterator<Row> myIterator;
        private final Predicate<RepairEntry> myPredicate;

        RepairEntryIterator(final Iterator<Row> iterator, final Predicate<RepairEntry> predicate)
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

                if (!validateFields(row))
                {
                    continue;
                }

                LongTokenRange tokenRange = parseTokenRange(row);
                Set<DriverNode> nodes = resolveNodes(row);
                String status = row.getString(STATUS_COLUMN);
                long startedAt = row.getInstant(STARTED_AT_COLUMN).toEpochMilli();
                long finishedAt = parseFinishedAt(row);

                RepairEntry repairEntry = new RepairEntry(tokenRange, startedAt, finishedAt, nodes, status);

                if (myPredicate.apply(repairEntry))
                {
                    return repairEntry;
                }
            }

            return endOfData();
        }

        private LongTokenRange parseTokenRange(final Row row)
        {
            long rangeBegin = Long.parseLong(row.getString(RANGE_BEGIN_COLUMN));
            long rangeEnd = Long.parseLong(row.getString(RANGE_END_COLUMN));
            return new LongTokenRange(rangeBegin, rangeEnd);
        }

        private Set<DriverNode> resolveNodes(final Row row)
        {
            Set<DriverNode> nodes = new HashSet<>();
            Set<InetAddress> participants = row.getSet(PARTICIPANTS_COLUMN, InetAddress.class);

            InetAddress coordinator = row.get(COORDINATOR_COLUMN, InetAddress.class);
            resolveNode(coordinator).ifPresent(nodes::add);

            for (InetAddress participant : participants)
            {
                resolveNode(participant).ifPresent(nodes::add);
            }

            return nodes;
        }

        private Optional<DriverNode> resolveNode(final InetAddress address)
        {
            Optional<DriverNode> node = myNodeResolver.fromIp(address);
            if (!node.isPresent())
            {
                LOG.warn("Node {} not found in metadata", address);
            }
            return node;
        }

        private long parseFinishedAt(final Row row)
        {
            Instant finished = row.getInstant(FINISHED_AT_COLUMN);
            return (finished != null) ? finished.toEpochMilli() : -1L;
        }

        private boolean validateFields(final Row row)
        {
            return !row.isNull(PARTICIPANTS_COLUMN)
                    && !row.isNull(RANGE_BEGIN_COLUMN)
                    && !row.isNull(RANGE_END_COLUMN)
                    && !row.isNull(COORDINATOR_COLUMN)
                    && !row.isNull(STARTED_AT_COLUMN);
        }
    }
}
