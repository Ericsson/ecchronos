/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;

/**
 * Interface for policies that can be used to control if repairs should run.
 */
@FunctionalInterface
public interface TableRepairPolicy
{
    /**
     * Check with the policy if a repair of the provided table should run now.
     *
     * @param tableReference The table to verify.
     * @return True if the repair should continue.
     */
    boolean shouldRun(TableReference tableReference);
}
