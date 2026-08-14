/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.table;

import java.util.Set;

/**
 * A record representing a time-based run policy rejection bucket.
 * Defines a time window during which repairs are rejected for a specific keyspace and table,
 * optionally limited to certain datacenters.
 *
 * @param keyspaceName the keyspace name this rejection applies to.
 * @param tableName the table name this rejection applies to.
 * @param startHour the start hour of the rejection window.
 * @param startMinute the start minute of the rejection window.
 * @param endHour the end hour of the rejection window.
 * @param endMinute the end minute of the rejection window.
 * @param dcExclusions the set of datacenter names to exclude during this window.
 */
public record TimeBasedRunPolicyBucket(
    String keyspaceName,
    String tableName,
    Integer startHour,
    Integer startMinute,
    Integer endHour,
    Integer endMinute,
    Set<String> dcExclusions
)
{
}
