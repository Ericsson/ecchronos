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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import static com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory.DistributedLock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;

/**
 * Represents a container for builder configurations and state for the CASLock.
 * This class is used to decouple builder fields from CASLockFactory to avoid excessive field count.
 */
class CASLock implements DistributedLock, Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(CASLock.class);

    private final String myResource;
    private final int myPriority;
    private final Map<String, String> myMetadata;

    private final AtomicReference<ScheduledFuture<?>> myUpdateFuture = new AtomicReference<>();

    private final AtomicInteger myFailedUpdateAttempts = new AtomicInteger();

    private final int myLocallyHighestPriority;
    private final int globalHighPriority;

    private final UUID myUuid;

    private final CASLockStatement myCasLockStatement;

    CASLock(final String resource,
            final int priority,
            final Map<String, String> metadata,
            final UUID uuid,
            final CASLockStatement casLockStatement)
    {
        myResource = resource;
        myPriority = priority;
        myMetadata = metadata;
        myUuid = uuid;
        myCasLockStatement = casLockStatement;

        List<NodePriority> nodePriorities = computePriorities();

        myLocallyHighestPriority = nodePriorities.stream()
                .filter(n -> n.getUuid().equals(myUuid))
                .map(NodePriority::getPriority)
                .findFirst()
                .orElse(myPriority);
        globalHighPriority = nodePriorities.stream()
                .filter(n -> !n.getUuid().equals(myUuid))
                .map(NodePriority::getPriority)
                .max(Integer::compare)
                .orElse(myPriority);
    }

    public boolean lock()
    {
        if (compete())
        {
            LOG.trace("Trying to acquire lock for resource {}", myResource);
            if (tryLock())
            {
                ScheduledExecutorService executor = myCasLockStatement.getCasLockProperties().getExecutor();
                LOG.trace("Lock for resource {} acquired", myResource);
                ScheduledFuture<?> future = executor.scheduleAtFixedRate(this,
                        myCasLockStatement.getCasLockFactoryCacheContext().getLockUpdateTimeInSeconds(),
                        myCasLockStatement.getCasLockFactoryCacheContext().getLockUpdateTimeInSeconds(), TimeUnit.SECONDS);
                myUpdateFuture.set(future);

                return true;
            }
        }

        return false;
    }

    @Override
    public void run()
    {
        try
        {
            updateLock();
            myFailedUpdateAttempts.set(0);
        }
        catch (LockException e)
        {
            int failedAttempts = myFailedUpdateAttempts.incrementAndGet();

            if (failedAttempts >= myCasLockStatement.getCasLockFactoryCacheContext().getFailedLockRetryAttempts())
            {
                LOG.error("Unable to re-lock resource '{}' after {} failed attempts", myResource, failedAttempts);
            }
            else
            {
                LOG.warn("Unable to re-lock resource '{}', {} failed attempts", myResource, failedAttempts, e);
            }
        }
    }

    @Override
    public void close()
    {
        ScheduledFuture<?> future = myUpdateFuture.get();
        if (future != null)
        {
            future.cancel(true);
            myCasLockStatement.execute(
                    myCasLockStatement.getRemoveLockStatement().bind(myResource, myUuid));

            if (myLocallyHighestPriority <= myPriority)
            {
                myCasLockStatement.execute(
                        myCasLockStatement.getRemoveLockPriorityStatement().bind(myResource, myUuid));
            }
            else
            {
                LOG.debug("Locally highest priority ({}) is higher than current ({}), will not remove",
                        myLocallyHighestPriority,
                        myPriority);
            }
        }
    }

    private void updateLock() throws LockException
    {
        ResultSet resultSet = myCasLockStatement.execute(
                myCasLockStatement.getUpdateLockStatement().bind(myUuid, myMetadata, myResource, myUuid));

        if (!resultSet.wasApplied())
        {
            throw new LockException("CAS query failed");
        }
    }

    private boolean compete()
    {
        if (myLocallyHighestPriority <= myPriority)
        {
            insertPriority();
        }

        LOG.trace("Highest priority for resource {}: {}", myResource, globalHighPriority);
        return myPriority >= globalHighPriority;
    }

    private void insertPriority()
    {
        myCasLockStatement.execute(
                myCasLockStatement.getCompeteStatement().bind(myResource, myUuid, myPriority));
    }

    private boolean tryLock()
    {
        return myCasLockStatement.execute(
                myCasLockStatement.getLockStatement().bind(myResource, myUuid, myMetadata)).wasApplied();
    }

    private List<NodePriority> computePriorities()
    {
        List<NodePriority> nodePriorities = new ArrayList<>();

        ResultSet resultSet = myCasLockStatement.execute(
                myCasLockStatement.getGetPriorityStatement().bind(myResource));

        for (Row row : resultSet)
        {
            int priority = row.getInt(CASLockStatement.COLUMN_PRIORITY);
            UUID hostId = row.getUuid(CASLockStatement.COLUMN_NODE);

            nodePriorities.add(new NodePriority(hostId, priority));
        }

        return nodePriorities;
    }

    int getFailedAttempts()
    {
        return myFailedUpdateAttempts.get();
    }
}
