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
package com.ericsson.bss.cassandra.ecchronos.core.repair.types;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;

import java.net.InetAddress;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A representation of a virtual node state.
 *
 * Primarily used to to have a type to convert to JSON.
 */
public class VirtualNodeState
{
    public final long startToken;
    public final long endToken;
    public final Set<InetAddress> replicas;
    public final long lastRepairedAtInMs;
    public final boolean repaired;

    public VirtualNodeState(long startToken, long endToken, Set<InetAddress> replicas, long lastRepairedAtInMs, boolean repaired)
    {
        this.startToken = startToken;
        this.endToken = endToken;
        this.replicas = replicas;
        this.lastRepairedAtInMs = lastRepairedAtInMs;
        this.repaired = repaired;
    }

    public static VirtualNodeState convert(VnodeRepairState vnodeRepairState, long repairedAfter)
    {
        long startToken = vnodeRepairState.getTokenRange().start;
        long endToken = vnodeRepairState.getTokenRange().end;
        Set<InetAddress> replicas = vnodeRepairState.getReplicas().stream().map(Host::getBroadcastAddress).collect(Collectors.toSet());
        long lastRepairedAt = vnodeRepairState.lastRepairedAt();
        boolean repaired = lastRepairedAt > repairedAfter;

        return new VirtualNodeState(startToken, endToken, replicas, lastRepairedAt, repaired);
    }
}
