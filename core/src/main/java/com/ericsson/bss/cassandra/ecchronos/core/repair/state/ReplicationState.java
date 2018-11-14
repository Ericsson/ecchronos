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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;

public class ReplicationState
{
    private final Metadata myMetadata;
    private final Host myLocalHost;

    public ReplicationState(Metadata metadata, Host localhost)
    {
        myMetadata = metadata;
        myLocalHost = localhost;
    }

    public Map<LongTokenRange, ImmutableSet<Host>> getTokenRangeToReplicas(TableReference tableReference)
    {
        Map<LongTokenRange, ImmutableSet<Host>> tokenRangeToReplicaMap = new HashMap<>();

        for (TokenRange tokenRange : myMetadata.getTokenRanges(tableReference.getKeyspace(), myLocalHost))
        {
            LongTokenRange longTokenRange = convert(tokenRange);
            ImmutableSet<Host> hosts = ImmutableSet.copyOf(myMetadata.getReplicas(tableReference.getKeyspace(), tokenRange));
            tokenRangeToReplicaMap.put(longTokenRange, hosts);
        }

        return ImmutableMap.copyOf(tokenRangeToReplicaMap);
    }

    private LongTokenRange convert(TokenRange range)
    {
        // Assuming murmur3 partitioner
        long start = (long) range.getStart().getValue();
        long end = (long) range.getEnd().getValue();
        return new LongTokenRange(start, end);
    }
}
