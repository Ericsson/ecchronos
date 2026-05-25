/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.vnode;

import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.google.common.collect.ImmutableSet;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Interns {@link ImmutableSet} instances of {@link DriverNode} so that identical replica
 * combinations share the same object reference across the application lifecycle.
 * <p>
 * In a typical cluster with RF=3 and 6 nodes, there are at most 20 unique replica set
 * combinations. This cache ensures those ~20 sets are reused across the thousands of
 * {@code VnodeRepairState} instances that reference them.
 * <p>
 * The cache should be cleared on topology changes to avoid retaining obsolete combinations.
 */
public class ReplicaSetCache
{
    private final ConcurrentHashMap<ImmutableSet<DriverNode>, ImmutableSet<DriverNode>> myCache
            = new ConcurrentHashMap<>();

    /**
     * Returns a cached instance equal to the given replica set.
     *
     * @param replicas The replica set to intern.
     * @return A shared instance equal to the input.
     */
    public ImmutableSet<DriverNode> intern(final ImmutableSet<DriverNode> replicas)
    {
        return myCache.computeIfAbsent(replicas, k -> k);
    }

    /**
     * Clears the cache. Should be called on topology changes.
     */
    public void clear()
    {
        myCache.clear();
    }

    /**
     * Returns the current number of cached replica sets.
     *
     * @return The cache size.
     */
    public int size()
    {
        return myCache.size();
    }
}
