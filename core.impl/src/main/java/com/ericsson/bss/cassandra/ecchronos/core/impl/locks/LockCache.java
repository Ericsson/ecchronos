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

import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory.DistributedLock;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.LockException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import jakarta.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache for lock acquisition failures. Prevents repeated lock attempts for resources
 * that recently failed, with a configurable expiration time.
 */
public final class LockCache
{
    private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);
    private static final int MAX_CACHE_SIZE = 10000;

    private final Cache<LockKey, LockException> myFailureCache;
    private final LockSupplier myLockSupplier;

    /**
     * Constructs a LockCache with the specified lock supplier and expiration time in seconds.
     *
     * @param lockSupplier the supplier used to acquire locks.
     * @param expireTimeInSeconds the time in seconds after which cached failures expire.
     */
    public LockCache(final LockSupplier lockSupplier, final long expireTimeInSeconds)
    {
        this(lockSupplier, expireTimeInSeconds, TimeUnit.SECONDS);
    }

    LockCache(final LockSupplier lockSupplier, final long expireTime, final TimeUnit expireTimeUnit)
    {
        myLockSupplier = lockSupplier;

        myFailureCache = Caffeine.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(expireTime, expireTimeUnit)
                .executor(Runnable::run)
                .build();
    }

    /**
     * Gets the cached lock failure for the specified data center and resource, if any.
     *
     * @param dataCenter the data center name.
     * @param resource the resource name.
     * @return an optional containing the cached lock exception, or empty if no failure is cached.
     */
    public Optional<LockException> getCachedFailure(final String dataCenter, final String resource)
    {
        return getCachedFailure(new LockKey(dataCenter, resource));
    }

    /**
     * Attempts to acquire a distributed lock, checking the cache first for known failures.
     * If a lock attempt fails, the failure is cached.
     *
     * @param dataCenter the data center name.
     * @param resource the resource name.
     * @param priority the lock priority.
     * @param metadata metadata associated with the lock.
     * @return the acquired distributed lock.
     * @throws LockException if the lock cannot be acquired.
     */
    public DistributedLock getLock(final String dataCenter,
                                   final String resource,
                                   final int priority,
                                   final Map<String, String> metadata)
                                                                       throws LockException
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

    private void throwCachedLockException(final LockException e) throws LockException
    {
        LOG.debug("Encountered cached locking failure, throwing exception", e);
        throw new LockException("Cached: " + e.getMessage(), e);
    }

    private Optional<LockException> getCachedFailure(final LockKey lockKey)
    {
        return Optional.ofNullable(myFailureCache.getIfPresent(lockKey));
    }

    /**
     * Functional interface for supplying distributed locks.
     */
    @FunctionalInterface
    public interface LockSupplier
    {
        /**
         * Acquires a distributed lock for the given data center and resource.
         *
         * @param dataCenter the data center name.
         * @param resource the resource name.
         * @param priority the lock priority.
         * @param metadata metadata associated with the lock.
         * @return the acquired distributed lock.
         * @throws LockException if the lock cannot be acquired.
         */
        DistributedLock getLock(String dataCenter, String resource, int priority, Map<String, String> metadata)
            throws LockException;
    }

    record LockKey(String dataCenter, @NotNull String resource)
    {
        LockKey
        {
            checkNotNull(resource);
        }
    }
}
