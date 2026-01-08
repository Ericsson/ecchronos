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
package com.ericsson.bss.cassandra.ecchronos.core.repair.config;

/**
 * The repair options available for the repair.
 */
@SuppressWarnings({"PMD.DataClass", "checkstyle:hideutilityclassconstructor"})
public class RepairOptions
{
    /**
     * Default constructor.
     */
    public RepairOptions()
    {
    }

    /**
     * The {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism repair parallelism} to use for the repair.
     * <p>
     * Possible values are defined in {@link com.ericsson.bss.cassandra.ecchronos.utils.enums.repair.RepairParallelism}
     */
    public static final String PARALLELISM_KEY = "parallelism";

    /**
     * If the repair should be on the primary range of the node.
     * <p>
     * Possible values: true | false
     */
    public static final String PRIMARY_RANGE_KEY = "primaryRange";

    /**
     * If the repair should be incremental.
     * <p>
     * Possible values: true | false
     */
    public static final String INCREMENTAL_KEY = "incremental";

    public static final String UNREPLICATED_KEY = "ignoreUnreplicatedKeyspaces";

    /**
     * If the repair should be on a certain list of ranges.
     * <p>
     * If this option is used the repair will not be incremental.
     * <p>
     * The values should be of the format: [startToken1]:[endToken1],[startToken2]:[endToken2]
     */
    public static final String RANGES_KEY = "ranges";

    /**
     * The tables that should be repaired.
     */
    public static final String COLUMNFAMILIES_KEY = "columnFamilies";

    /**
     * The hosts that should be repaired.
     * <p>
     * If this option is used the repair will not be incremental.
     * <p>
     * The values should be of the format: [ip1],[ip2]
     */
    public static final String HOSTS_KEY = "hosts";
}
