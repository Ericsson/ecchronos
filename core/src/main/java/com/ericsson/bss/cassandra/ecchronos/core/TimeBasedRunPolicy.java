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

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.ericsson.bss.cassandra.ecchronos.connection.StatementDecorator;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.TableRepairPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.RunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.ScheduledJob;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

/**
 * Time based run policy
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
    private final Session mySession;
    private final Clock myClock;
    private final LoadingCache<TableReference, TimeRejectionCollection> myTimeRejectionCache;

    public TimeBasedRunPolicy(Builder builder)
    {
        mySession = builder.mySession;
        myStatementDecorator = builder.myStatementDecorator;
        myClock = builder.myClock;

        myGetRejectionsStatement = mySession.prepare(QueryBuilder.select()
                .from(builder.myKeyspaceName, TABLE_REJECT_CONFIGURATION)
                .where(eq("keyspace_name", bindMarker()))
                .and(eq("table_name", bindMarker())));

        myTimeRejectionCache = createConfigCache(builder.myCacheExpireTime);
    }

    private LoadingCache<TableReference, TimeRejectionCollection> createConfigCache(long expireAfter)
    {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(expireAfter, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<TableReference, TimeRejectionCollection>()
                {
                    @Override
                    public TimeRejectionCollection load(TableReference key)
                    {
                        Statement decoratedStatement = myStatementDecorator.apply(myGetRejectionsStatement.bind(key.getKeyspace(), key.getTable()));

                        ResultSet resultSet = mySession.execute(decoratedStatement);
                        Iterator<Row> iterator = resultSet.iterator();
                        return new TimeRejectionCollection(iterator);
                    }
                });
    }

    @Override
    public long validate(ScheduledJob job)
    {
        if (job instanceof TableRepairJob)
        {
            TableRepairJob repairJob = (TableRepairJob) job;

            return getRejectionsForTable(repairJob.getTableReference());
        }

        return -1;
    }

    @Override
    public boolean shouldRun(TableReference tableReference)
    {
        return getRejectionsForTable(tableReference) == -1L;
    }

    @Override
    public void close()
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

        private Session mySession;
        private StatementDecorator myStatementDecorator;
        private String myKeyspaceName = DEFAULT_KEYSPACE_NAME;
        private long myCacheExpireTime = DEFAULT_CACHE_EXPIRE_TIME;
        private Clock myClock = Clock.systemDefaultZone();

        public Builder withSession(Session session)
        {
            mySession = session;
            return this;
        }

        public Builder withStatementDecorator(StatementDecorator statementDecorator)
        {
            myStatementDecorator = statementDecorator;
            return this;
        }

        public Builder withKeyspaceName(String keyspaceName)
        {
            myKeyspaceName = keyspaceName;
            return this;
        }

        @VisibleForTesting
        Builder withCacheExpireTime(long expireTime)
        {
            myCacheExpireTime = expireTime;
            return this;
        }

        @VisibleForTesting
        Builder withClock(Clock clock)
        {
            myClock = clock;
            return this;
        }

        public TimeBasedRunPolicy build()
        {
            verifySchemasExists();
            return new TimeBasedRunPolicy(this);
        }

        private void verifySchemasExists()
        {
            KeyspaceMetadata keyspaceMetadata = mySession.getCluster().getMetadata().getKeyspace(myKeyspaceName);
            if (keyspaceMetadata == null)
            {
                LOG.error("Keyspace {} does not exist, it needs to be created", myKeyspaceName);
                throw new IllegalStateException("Keyspace " + myKeyspaceName + " does not exist, it needs to be created");
            }

            if (keyspaceMetadata.getTable(TABLE_REJECT_CONFIGURATION) == null)
            {
                LOG.error("Table {}.{} does not exist, it needs to be created", myKeyspaceName, TABLE_REJECT_CONFIGURATION);
                throw new IllegalStateException("Table " + myKeyspaceName + "." + TABLE_REJECT_CONFIGURATION + " does not exist, it needs to be created");
            }
        }
    }

    @VisibleForTesting
    void clearCache()
    {
        myTimeRejectionCache.invalidateAll();
    }

    class TimeRejectionCollection
    {
        private final List<TimeRejection> myRejections = new ArrayList<>();

        TimeRejectionCollection(Iterator<Row> iterator)
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

        TimeRejection(Row row)
        {
            myStart = toDateTime(row.getInt("start_hour"), row.getInt("start_minute"));
            myEnd = toDateTime(row.getInt("end_hour"), row.getInt("end_minute"));
        }

        public long rejectionTime()
        {
            // 00:00->00:00 means that we pause the repair scheduling, so wait DEFAULT_REJECT_TIME instead of until 00:00
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

        private LocalDateTime toDateTime(int h, int m)
        {
            return LocalDateTime.now(myClock)
                    .withHour(h)
                    .withMinute(m)
                    .withSecond(0);
        }
    }

    private long getRejectionsForTable(TableReference tableReference)
    {
        long rejectTime = -1L;
        try
        {
            TableReference[] tableKeys = new TableReference[]
                    {
                            new TableReference("*", "*"),
                            new TableReference("*", tableReference.getTable()),
                            tableReference
                    };

            for (int i = 0; i < tableKeys.length && rejectTime == -1L; i++)
            {
                rejectTime = myTimeRejectionCache.get(tableKeys[i]).rejectionTime();
            }
        }
        catch (Exception e)
        {
            LOG.error("Unable to parse/fetch rejection time for {}", tableReference, e);
            rejectTime = DEFAULT_REJECT_TIME;
        }

        return rejectTime;
    }
}
