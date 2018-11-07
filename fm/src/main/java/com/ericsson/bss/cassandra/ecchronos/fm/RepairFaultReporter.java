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
package com.ericsson.bss.cassandra.ecchronos.fm;

import java.util.Map;

public interface RepairFaultReporter
{
    String FAULT_KEYSPACE = "KEYSPACE";
    String FAULT_TABLE = "TABLE";

    enum FaultCode
    {
        REPAIR_WARNING,
        REPAIR_ERROR
    }

    void raise(FaultCode faultCode, Map<String, Object> data);

    void cease(FaultCode faultCode, Map<String, Object> data);
}
