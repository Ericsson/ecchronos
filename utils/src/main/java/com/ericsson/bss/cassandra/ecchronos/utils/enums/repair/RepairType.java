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
package com.ericsson.bss.cassandra.ecchronos.utils.enums.repair;

/**
 * The type of repair used.
 */
public enum RepairType
{
    /** Standard vnode-based repair, repairing one vnode range at a time. */
    VNODE,
    /** Parallel vnode-based repair, repairing multiple vnode ranges concurrently. */
    PARALLEL_VNODE,
    /** Incremental repair, repairing only data that has changed since last repair. */
    INCREMENTAL,
    /**
     * Unified vnode-based repair. Functionally equivalent to {@link #VNODE} at the repair level, but managed
     * by the consolidated multi-node scheduling subsystem: a single repair job per table coordinates all of
     * its nodes, replacing the per-node job/thread model. Opt-in, side by side with {@link #VNODE}.
     */
    UNIFIED_VNODE;

    /**
     * Whether this repair type belongs to the consolidated {@code unified_*} family, which is handled by the
     * multi-node scheduling subsystem rather than the legacy per-node subsystem.
     *
     * @return {@code true} if this is a unified repair type.
     */
    public boolean isUnified()
    {
        return this == UNIFIED_VNODE;
    }
}
