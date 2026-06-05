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

/** Reporter interface for raising and clearing repair-related fault alarms. */
public interface RepairFaultReporter
{
    /** Key used to identify the keyspace in fault data. */
    String FAULT_KEYSPACE = "KEYSPACE";
    /** Key used to identify the table in fault data. */
    String FAULT_TABLE = "TABLE";

    /** Fault codes indicating the severity of a repair issue. */
    enum FaultCode
    {
        /** Indicates a repair warning condition. */
        REPAIR_WARNING,
        /** Indicates a repair error condition. */
        REPAIR_ERROR
    }

    /**
     * This method might be called multiple times with the same parameters,
     * implementations of this method should control whether the alarm should be raised.
     * @param faultCode The fault code
     * @param data The data containing keyspace and table
     */
    void raise(FaultCode faultCode, Map<String, Object> data);

    /**
     * This method might be called multiple times with the same parameters,
     * implementations of this method should control whether the alarm should be cleared.
     * @param faultCode The fault code
     * @param data The data containing keyspace and table
     */
    void cease(FaultCode faultCode, Map<String, Object> data);
}
