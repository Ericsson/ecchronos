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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

/**
 * Implementation of the RepairHistoryProvider interface that retrieves the repair history from Cassandra.
 */
public class RepairHistoryProviderImpl implements RepairHistoryProvider
{
    private static final String RANGE_BEGIN_COLUMN = "range_begin";
    private static final String RANGE_END_COLUMN = "range_end";
    private static final String ID_COLUMN = "id";
    private static final String STATUS_COLUMN = "status";
    private static final String PARTICIPANTS_COLUMN = "participants";

    private static final String KEYSPACE_NAME = "system_distributed";
    private static final String REPAIR_HISTORY = "repair_history";

    private static final String REPAIR_HISTORY_BY_TIME_STATEMENT = String
            .format("SELECT id, range_begin, range_end, status, participants FROM %s.%s WHERE keyspace_name=? AND columnfamily_name=? AND id >= minTimeuuid(?) and id <= maxTimeuuid(?)", KEYSPACE_NAME, REPAIR_HISTORY);
    private static final String REPAIR_HISTORY_STATEMENT = String.format("SELECT id, range_begin, range_end, status, participants FROM %s.%s WHERE keyspace_name=? AND columnfamily_name=? AND id <= maxTimeuuid(?)", KEYSPACE_NAME, REPAIR_HISTORY);

    private final Session mySession;
    private final StatementDecorator myStatementDecorator;

    private final PreparedStatement myRepairHistoryByTimeStatement;
    private final PreparedStatement myRepairHistoryStatement;

    public RepairHistoryProviderImpl(Session session, StatementDecorator statementDecorator)
    {
        mySession = session;
        myStatementDecorator = statementDecorator;
        myRepairHistoryByTimeStatement = mySession.prepare(REPAIR_HISTORY_BY_TIME_STATEMENT);
        myRepairHistoryStatement = mySession.prepare(REPAIR_HISTORY_STATEMENT);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate)
    {
        ResultSet resultSet = execute(myRepairHistoryStatement.bind(tableReference.getKeyspace(), tableReference.getTable(), new Date(to)));

        return RepairEntryIterator.create(resultSet.iterator(), predicate);
    }

    @Override
    public Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from, Predicate<RepairEntry> predicate)
    {
        ResultSet resultSet = execute(myRepairHistoryByTimeStatement.bind(tableReference.getKeyspace(), tableReference.getTable(), new Date(from), new Date(to)));

        return RepairEntryIterator.create(resultSet.iterator(), predicate);
    }

    private ResultSet execute(Statement statement)
    {
        return mySession.execute(myStatementDecorator.apply(statement));
    }

    private static class RepairEntryIterator extends AbstractIterator<RepairEntry>
    {
        private final Iterator<Row> myIterator;
        private final Predicate<RepairEntry> myPredicate;

        public static RepairEntryIterator create(Iterator<Row> iterator, Predicate<RepairEntry> predicate)
        {
            return new RepairEntryIterator(iterator, predicate);
        }

        private RepairEntryIterator(Iterator<Row> iterator, Predicate<RepairEntry> predicate)
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
                    UUID id = row.getUUID(ID_COLUMN);
                    Set<InetAddress> participants = row.getSet(PARTICIPANTS_COLUMN, InetAddress.class);
                    String status = row.getString(STATUS_COLUMN);

                    long startedAt = UUIDs.unixTimestamp(id);

                    RepairEntry repairEntry = new RepairEntry(tokenRange, startedAt, participants, status);

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
                    !row.isNull(ID_COLUMN);
        }
    }
}
