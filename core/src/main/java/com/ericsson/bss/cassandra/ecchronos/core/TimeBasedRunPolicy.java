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
package com.ericsson.bss.cassandra.ecchronos.core;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

/**
 * Time based run policy.
 *
 * Expected keyspace/table:
 * CREATE KEYSPACE IF NOT EXISTS ecchronos WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
 *
 * CREATE TABLE IF NOT EXISTS ecchronos.reject_configuration(
 * keyspace_name text,
 * table_name text,
 * start_hour int,
 * start_minute int,
 * end_hour int,
 * end_minute int,
 * PRIMARY KEY(keyspace_name, table_name, start_hour, start_minute));
 */
public class TimeBasedRunPolicy implements TableRepairPolicy, RunPolicy, Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TimeBasedRunPolicy.class);

    private static final String TABLE_REJECT_CONFIGURATION = "reject_configuration";

    private static final long DEFAULT_REJECT_TIME = TimeUnit.MINUTES.toMillis(1);

    static final long DEFAULT_CACHE_EXPIRE_TIME = TimeUnit.SECONDS.toMillis(10);

    private final PreparedStatement myGetRejectionsStatement;
    private final StatementDecorator myStatementDecorator;
    private final CqlSession mySession;
    private final Clock myClock;
    private final LoadingCache<TableKey, TimeRejectionCollection> myTimeRejectionCache;

    public TimeBasedRunPolicy(final Builder builder)
    {
        mySession = builder.mySession;
        myStatementDecorator = builder.myStatementDecorator;
        myClock = builder.myClock;

        myGetRejectionsStatement = mySession.prepare(
                QueryBuilder.selectFrom(builder.myKeyspaceName, TABLE_REJECT_CONFIGURATION)
                .all()
                .whereColumn("keyspace_name")
                .isEqualTo(bindMarker())
                .whereColumn("table_name").isEqualTo(bindMarker())
                .build());

        myTimeRejectionCache = createConfigCache(builder.myCacheExpireTime);
    }

    private LoadingCache<TableKey, TimeRejectionCollection> createConfigCache(final long expireAfter)
    {
        return Caffeine.newBuilder()
                .expireAfterWrite(expireAfter, TimeUnit.MILLISECONDS)
                .executor(Runnable::run)
                .build(key -> load(key));
    }

    private TimeRejectionCollection load(final TableKey key)
    {
        Statement decoratedStatement =
                myStatementDecorator.apply(myGetRejectionsStatement.bind(key.getKeyspace(),
                        key.getTable()));

        ResultSet resultSet = mySession.execute(decoratedStatement);
        Iterator<Row> iterator = resultSet.iterator();
        return new TimeRejectionCollection(iterator);
    }

    @Override
    public final long validate(final ScheduledJob job)
    {
        if (job instanceof TableRepairJob)
        {
            TableRepairJob repairJob = (TableRepairJob) job;

            return getRejectionsForTable(repairJob.getTableReference());
        }

        return -1;
    }

    @Override
    public final boolean shouldRun(final TableReference tableReference)
    {
        return getRejectionsForTable(tableReference) == -1L;
    }

    @Override
    public final void close()
    {
        myTimeRejectionCache.invalidateAll();
        myTimeRejectionCache.cleanUp();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private static final String DEFAULT_KEYSPACE_NAME = "ecchronos";

        private CqlSession mySession;
        private StatementDecorator myStatementDecorator;
        private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
        private long myCacheExpireTime = DEFAULT_CACHE_EXPIRE_TIME;
        private Clock myClock = Clock.systemDefaultZone();

        public final Builder withSession(final CqlSession session)
        {
            mySession = session;
            return this;
        }

        public final Builder withStatementDecorator(final StatementDecorator statementDecorator)
        {
            myStatementDecorator = statementDecorator;
            return this;
        }

        public final Builder withKeyspaceName(final String keyspaceName)
        {
            myKeyspaceName = keyspaceName;
            return this;
        }

        /**
         * Also visible for testing.
         */
        @VisibleForTesting
        Builder withCacheExpireTime(final long expireTime)
        {
            myCacheExpireTime = expireTime;
            return this;
        }

        /**
         * Also visible for testing.
         */
        @VisibleForTesting
        Builder withClock(final Clock clock)
        {
            myClock = clock;
            return this;
        }

        public final TimeBasedRunPolicy build()
        {
            verifySchemasExists();
            return new TimeBasedRunPolicy(this);
        }

        private void verifySchemasExists()
        {
            Optional<KeyspaceMetadata> keyspaceMetadata = mySession.getMetadata().getKeyspace(myKeyspaceName);
            if (!keyspaceMetadata.isPresent())
            {
                String msg = String.format("Keyspace %s does not exist, it needs to be created", myKeyspaceName);
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }

            if (!keyspaceMetadata.get().getTable(TABLE_REJECT_CONFIGURATION).isPresent())
            {
                String msg = String.format("Table %s.%s does not exist, it needs to be created",
                        myKeyspaceName,
                        TABLE_REJECT_CONFIGURATION);
                LOG.error(msg);
                throw new IllegalStateException(msg);
            }
        }
    }

    /**
     * Also visible for testing.
     */
    @VisibleForTesting
    void clearCache()
    {
        myTimeRejectionCache.invalidateAll();
    }

    class TimeRejectionCollection
    {
        private final List<TimeRejection> myRejections = new ArrayList<>();

        TimeRejectionCollection(final Iterator<Row> iterator)
        {
            while (iterator.hasNext())
            {
                Row row = iterator.next();
                myRejections.add(new TimeRejection(row));
            }
        }

        public long rejectionTime()
        {
            for (TimeRejection rejection : myRejections)
            {
                long rejectionTime = rejection.rejectionTime();

                if (rejectionTime != -1L)
                {
                    return rejectionTime;
                }
            }

            return -1L;
        }
    }

    class TimeRejection
    {
        private final LocalDateTime myStart;
        private final LocalDateTime myEnd;

        TimeRejection(final Row row)
        {
            myStart = toDateTime(row.getInt("start_hour"), row.getInt("start_minute"));
            myEnd = toDateTime(row.getInt("end_hour"), row.getInt("end_minute"));
        }

        public long rejectionTime()
        {
            // 00:00->00:00 means that we pause the repair scheduling,
            // so wait DEFAULT_REJECT_TIME instead of until 00:00
            if (myStart.getHour() == 0
                    && myStart.getMinute() == 0
                    && myEnd.getHour() == 0
                    && myEnd.getMinute() == 0)
            {
                return DEFAULT_REJECT_TIME;
            }

            return calculateRejectTime();
        }

        private long calculateRejectTime()
        {
            LocalDateTime now = LocalDateTime.now(myClock);

            if (isWraparound())
            {
                if (now.isBefore(myEnd))
                {
                    return Duration.between(now, myEnd).toMillis();
                }
                else if (now.isAfter(myStart))
                {
                    return Duration.between(now, myEnd.plusDays(1)).toMillis();
                }
            }
            else if (now.isAfter(myStart) && now.isBefore(myEnd))
            {
                return Duration.between(now, myEnd).toMillis();
            }

            return -1L;
        }

        private boolean isWraparound()
        {
            return myEnd.isBefore(myStart);
        }

        private LocalDateTime toDateTime(final int h, final int m)
        {
            return LocalDateTime.now(myClock)
                    .withHour(h)
                    .withMinute(m)
                    .withSecond(0);
        }
    }

    private long getRejectionsForTable(final TableReference tableReference)
    {
        long rejectTime = -1L;
        try
        {
            TableKey[] tableKeys = new TableKey[]
                    {
                            allKeyspaces(),
                            allKeyspaces(tableReference.getTable()),
                            forTable(tableReference)
                    };

            for (int i = 0; i < tableKeys.length && rejectTime == -1L; i++)
            {
                rejectTime = myTimeRejectionCache.get(tableKeys[i]).rejectionTime();
            }
        }
        catch (Exception e)
        {
            LOG.warn("Unable to parse/fetch rejection time for {}", tableReference, e);
            rejectTime = DEFAULT_REJECT_TIME;
        }

        return rejectTime;
    }

    private TableKey allKeyspaces()
    {
        return new TableKey("*", "*");
    }

    private TableKey allKeyspaces(final String table)
    {
        return new TableKey("*", table);
    }

    private TableKey forTable(final TableReference tableReference)
    {
        return new TableKey(tableReference.getKeyspace(), tableReference.getTable());
    }

    static class TableKey
    {
        private final String keyspace;
        private final String table;

        TableKey(final String aKeyspace, final String aTable)
        {
            this.keyspace = aKeyspace;
            this.table = aTable;
        }

        String getKeyspace()
        {
            return keyspace;
        }

        String getTable()
        {
            return table;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            TableKey tableKey = (TableKey) o;
            return keyspace.equals(tableKey.keyspace) && table.equals(tableKey.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table);
        }
    }
}
