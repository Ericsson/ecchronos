/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory.DistributedLock;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class LockCache
{
    private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);

    private static final long DEFAULT_EXPIRE_TIME_IN_SECONDS = 30;

    private final Cache<LockKey, LockException> myFailureCache;
    private final LockSupplier myLockSupplier;

    public LockCache(LockSupplier lockSupplier)
    {
        this(lockSupplier, DEFAULT_EXPIRE_TIME_IN_SECONDS, TimeUnit.SECONDS);
    }

    LockCache(LockSupplier lockSupplier, long expireTime, TimeUnit expireTimeUnit)
    {
        myLockSupplier = lockSupplier;

        myFailureCache = CacheBuilder.newBuilder()
                .expireAfterWrite(expireTime, expireTimeUnit)
                .build();
    }

    public Optional<LockException> getCachedFailure(String dataCenter, String resource)
    {
        return getCachedFailure(new LockKey(dataCenter, resource));
    }

    public DistributedLock getLock(String dataCenter, String resource, int priority, Map<String, String> metadata) throws LockException
    {
        LockKey lockKey = new LockKey(dataCenter, resource);

        Optional<LockException> cachedFailure = getCachedFailure(lockKey);

        if (cachedFailure.isPresent())
        {
            throwCachedLockException(cachedFailure.get());
        }

        try
        {
            return myLockSupplier.getLock(dataCenter, resource, priority, metadata);
        }
        catch (LockException e)
        {
            myFailureCache.put(lockKey, e);
            throw e;
        }
    }

    private void throwCachedLockException(LockException e) throws LockException
    {
        LOG.debug("Encountered cached locking failure, throwing exception", e);
        throw e;
    }

    private Optional<LockException> getCachedFailure(LockKey lockKey)
    {
        return Optional.ofNullable(myFailureCache.getIfPresent(lockKey));
    }

    @FunctionalInterface
    public interface LockSupplier
    {
        DistributedLock getLock(String dataCenter, String resource, int priority, Map<String, String> metadata) throws LockException;
    }

    static final class LockKey
    {
        private final String myDataCenter;
        private final String myResourceName;

        LockKey(String dataCenter, String resourceName)
        {
            myDataCenter = dataCenter;
            myResourceName = checkNotNull(resourceName);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockKey lockKey = (LockKey) o;
            return Objects.equals(myDataCenter, lockKey.myDataCenter) &&
                    Objects.equals(myResourceName, lockKey.myResourceName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(myDataCenter, myResourceName);
        }
    }
}
