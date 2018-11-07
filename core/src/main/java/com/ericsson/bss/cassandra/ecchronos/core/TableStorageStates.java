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
package com.ericsson.bss.cassandra.ecchronos.core;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Interface for retrieving storage usage for all tables this nodes should repair.
 */
public interface TableStorageStates
{
    /**
     * @param tableReference
     * @return The data size of the provided table on this node.
     */
    long getDataSize(TableReference tableReference);

    /**
     * @return The data size of all tables on this node.
     */
    long getDataSize();
}
