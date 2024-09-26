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
 * The type of repair parallelism used.
 */
public enum RepairParallelism
{
    /**
     * All nodes are repaired in parallel.
     */
    PARALLEL("parallel");

    private final String myName;

    RepairParallelism(final String name)
    {
        myName = name;
    }

    /**
     * Get the name of the repair parallelism.
     *
     * @return The repair parallelism name.
     */
    public String getName()
    {
        return myName;
    }
}
