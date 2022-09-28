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
package com.ericsson.bss.cassandra.ecchronos.core.metrics;

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

import java.util.Optional;

/**
 * Interface for providing table based repair metrics.
 */
public interface TableRepairMetricsProvider
{
    /**
     * @param tableReference the table
     * @return an Optional describing the repair ratio for the given table, or an empty Optional if the table was
     * not found.
     */
    Optional<Double> getRepairRatio(TableReference tableReference);
}
