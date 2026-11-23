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
 * REST interface for reading and changing log levels of the running ecChronos agent.
 *
 * Level changes are applied in memory only and are lost when the agent restarts.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface LoggingREST
{
    /**
     * Get all loggers that have an explicitly configured level, plus the root logger.
     *
     * @return a JSON object of the form
     *         {@code {"loggers": [{"logger": "<name>", "configured_level": "<level>", "effective_level": "<level>"}]}}
     */
    ResponseEntity<Map<String, Object>> getLoggers();

    /**
     * Get the configured and effective level of one logger. A logger without an explicit level has a
     * {@code null} configured level and inherits its effective level from its nearest ancestor.
     *
     * @param logger the logger name, {@code ROOT} for the root logger.
     * @return the logger state, or a bad request if the name is invalid.
     */
    ResponseEntity<Map<String, Object>> getLogger(String logger);

    /**
     * Set the level of one logger, or clear its explicit level.
     *
     * @param logger the logger name, {@code ROOT} for the root logger.
     * @param body a JSON object {@code {"level": "<level>"}} where the level is one of TRACE, DEBUG, INFO, WARN,
     *             ERROR, OFF or {@code reset}. {@code reset} clears the explicit level so the logger inherits again.
     * @return the new logger state, or a bad request (and no change) if the name or level is invalid.
     */
    ResponseEntity<Map<String, Object>> setLogger(String logger, Map<String, Object> body);
}
