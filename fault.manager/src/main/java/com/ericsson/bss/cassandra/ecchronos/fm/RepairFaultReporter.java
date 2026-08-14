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
package com.ericsson.bss.cassandra.ecchronos.fm;

import java.util.Map;

/**
 * Interface for reporting repair faults (alarms) such as warnings and errors.
 * Implementations control alarm deduplication and lifecycle.
 */
public interface RepairFaultReporter
{
    /** Key for the keyspace name in fault data maps. */
    String FAULT_KEYSPACE = "KEYSPACE";
    /** Key for the table name in fault data maps. */
    String FAULT_TABLE = "TABLE";
    /** Key for the node ID in fault data maps. */
    String FAULT_NODE_ID = "NODE_ID";

    /**
     * Fault codes indicating the severity of a repair fault.
     */
    enum FaultCode
    {
        /** A non-critical repair warning. */
        REPAIR_WARNING,
        /** A critical repair error. */
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
