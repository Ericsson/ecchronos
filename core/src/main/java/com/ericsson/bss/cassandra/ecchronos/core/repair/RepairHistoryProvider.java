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
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.Iterator;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.google.common.base.Predicate;

/**
 * Interface used to retrieve repair history.
 */
public interface RepairHistoryProvider
{

    /**
     * Iterate the repair history for the provided table starting from the {@code from} and going backwards.
     * The predicate is used to decide which repair entries should be filtered out of the result.
     *
     * @param tableReference The table for which the history should be iterated.
     * @param to The latest point in time to iterate to.
     * @param predicate The predicate used to filter out entries in the iterator results.
     * @return A filtered iterator for the repair history of the table.
     */
    Iterator<RepairEntry> iterate(TableReference tableReference, long to, Predicate<RepairEntry> predicate);

    /**
     * Iterate the repair history for the provided table starting from the {@code from} and going backwards until {@code to}.
     * The predicate is used to decide which repair entries should be filtered out of the result.
     *
     * @param tableReference The table for which the history should be iterated.
     * @param to The last point in time to iterate to.
     * @param from The point in time to start iterating from.
     * @param predicate The predicate used to filter out entries in the iterator results.
     * @return A filtered iterator for the repair history of the table.
     */
    Iterator<RepairEntry> iterate(TableReference tableReference, long to, long from, Predicate<RepairEntry> predicate);
}
