/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.rest;

import java.util.Map;

import org.springframework.http.ResponseEntity;

/**
 * REST interface for exposing the ecChronos agent version.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface VersionREST
{
    /**
     * Get the version of the running ecChronos agent.
     *
     * @return a JSON object of the form {@code {"version": "<version>"}}
     */
    ResponseEntity<Map<String, String>> getVersion();
}
