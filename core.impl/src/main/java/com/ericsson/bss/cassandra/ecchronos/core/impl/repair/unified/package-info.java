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
/**
 * The consolidated multi-node repair subsystem used by
 * {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairType#UNIFIED_VNODE}.
 * <p>
 * A single {@link com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified.UnifiedTableRepairJob} per
 * table coordinates all of its nodes (one {@link com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified.NodeRepairState}
 * per node), scheduled by a single-queue
 * {@link com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified.UnifiedScheduleManager} with an
 * elastic worker pool whose repair concurrency is governed by the distributed CAS locks. This subsystem is
 * opt-in and runs side by side with the legacy per-node vnode/incremental path.
 */
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.unified;
