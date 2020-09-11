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

import java.util.Map;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableSet;

/**
 * Replication state interface used to retrieve mappings between token range to responsible nodes.
 *
 * Within a keyspace the methods are expected to return the exact same object instance for a set of nodes.
 */
public interface ReplicationState
{
    /**
     * Get a map of the current replication state for the provided table.
     *
     * @param tableReference
     *            The table used to calculate the proper replication.
     * @return The map consisting of token -&gt; responsible nodes.
     */
    Map<LongTokenRange, ImmutableSet<Host>> getTokenRangeToReplicas(TableReference tableReference);
}
