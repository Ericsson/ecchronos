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
package com.ericsson.bss.cassandra.ecchronos.core.impl.utils;

/**
 * Defines the type of serial consistency to use for Compare-And-Set (CAS) lock operations.
 */
public enum ConsistencyType
{
    /** Local serial consistency, using LOCAL_SERIAL for CAS operations. */
    LOCAL,
    /** Full serial consistency, using SERIAL for CAS operations. */
    SERIAL
}
