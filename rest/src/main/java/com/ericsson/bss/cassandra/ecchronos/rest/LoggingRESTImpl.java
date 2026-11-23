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

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.logging.LogLevel;
import org.springframework.boot.logging.LoggerConfiguration;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for reading and changing log levels of the running ecChronos agent.
 *
 * Levels are applied through Spring Boot's {@link LoggingSystem}, so the change takes effect immediately and also
 * propagates to {@code java.util.logging} (embedded Tomcat) through logback's {@code LevelChangePropagator}.
 * Changes are in memory only and are lost when the agent restarts.
 */
@Tag(name = "Logging", description = "Read and change log levels of the running ecChronos agent")
@RestController
public final class LoggingRESTImpl implements LoggingREST
{
    static final String RESET = "reset";

    private static final Logger LOG = LoggerFactory.getLogger(LoggingRESTImpl.class);

    private static final String LOGGING_ENDPOINT = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/v2/logging";
    private static final String ROOT = LoggingSystem.ROOT_LOGGER_NAME;
    private static final String KEY_LEVEL = "level";
    private static final String ACCEPTED_LEVELS = "TRACE, DEBUG, INFO, WARN, ERROR, OFF or " + RESET;

    /** The levels that can be set. {@link LogLevel#FATAL} is deliberately excluded as logback has no such level. */
    private static final Map<String, LogLevel> SETTABLE_LEVELS = settableLevels();

    private final LoggingSystem myLoggingSystem;

    /**
     * Constructs the logging REST controller.
     *
     * @param loggingSystem the Spring Boot logging system used to read and change levels.
     */
    @Autowired
    public LoggingRESTImpl(final LoggingSystem loggingSystem)
    {
        myLoggingSystem = loggingSystem;
    }

    @Override
    @GetMapping(value = LOGGING_ENDPOINT, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-loggers", description = "Get the loggers with an explicit log level, plus ROOT",
            summary = "Get logging levels")
    public ResponseEntity<Map<String, Object>> getLoggers()
    {
        List<LoggerConfiguration> explicit = new ArrayList<>();
        for (LoggerConfiguration configuration : myLoggingSystem.getLoggerConfigurations())
        {
            if (configuration.getConfiguredLevel() != null || ROOT.equals(configuration.getName()))
            {
                explicit.add(configuration);
            }
        }
        // ROOT first, then alphabetical, so the output is stable.
        explicit.sort(Comparator.comparing((LoggerConfiguration c) -> !ROOT.equals(c.getName()))
                .thenComparing(LoggerConfiguration::getName));

        List<Map<String, Object>> loggers = new ArrayList<>();
        for (LoggerConfiguration configuration : explicit)
        {
            loggers.add(describe(configuration.getName(), configuration));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("loggers", loggers);
        return ResponseEntity.ok(body);
    }

    @Override
    @GetMapping(value = LOGGING_ENDPOINT + "/{logger}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-logger", description = "Get the configured and effective level of one logger",
            summary = "Get logging level of a logger")
    public ResponseEntity<Map<String, Object>> getLogger(@PathVariable("logger") final String logger)
    {
        String name;
        try
        {
            name = normalizeName(logger);
        }
        catch (IllegalArgumentException e)
        {
            return badRequest(e.getMessage());
        }
        return ResponseEntity.ok(describeWithInheritance(name));
    }

    @Override
    @PutMapping(value = LOGGING_ENDPOINT + "/{logger}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "set-logger", description = "Set or reset the level of one logger. The change is in "
            + "memory only and is lost when the agent restarts", summary = "Set logging level of a logger")
    public ResponseEntity<Map<String, Object>> setLogger(@PathVariable("logger") final String logger,
            @RequestBody(required = false) final Map<String, Object> body)
    {
        String name;
        LogLevel level;
        try
        {
            name = normalizeName(logger);
            level = parseLevel(body);
            if (level == null && ROOT.equals(name))
            {
                throw new IllegalArgumentException("The level of " + ROOT + " cannot be reset, set one of "
                        + "TRACE, DEBUG, INFO, WARN, ERROR or OFF instead");
            }
        }
        catch (IllegalArgumentException e)
        {
            return badRequest(e.getMessage());
        }

        myLoggingSystem.setLogLevel(name, level);
        LOG.info("Log level of logger '{}' {} via REST", name, level == null ? "reset" : "set to " + level);
        return ResponseEntity.ok(describeWithInheritance(name));
    }

    /**
     * Builds the name to level lookup of the levels that can be set. {@link LogLevel#FATAL} is deliberately excluded
     * as logback has no such level.
     */
    private static Map<String, LogLevel> settableLevels()
    {
        Map<String, LogLevel> levels = new LinkedHashMap<>();
        for (LogLevel level : EnumSet.complementOf(EnumSet.of(LogLevel.FATAL)))
        {
            levels.put(level.name(), level);
        }
        return Collections.unmodifiableMap(levels);
    }

    /**
     * Validates the logger name and maps any case of {@code root} to the root logger. A logger literally named
     * {@code root} would otherwise be a separate, useless logger next to {@code ROOT}.
     */
    private static String normalizeName(final String logger)
    {
        if (logger == null || logger.isBlank())
        {
            throw new IllegalArgumentException("The logger name must not be empty");
        }
        if (logger.chars().anyMatch(c -> Character.isWhitespace(c) || Character.isISOControl(c)))
        {
            throw new IllegalArgumentException("The logger name must not contain whitespace or control characters");
        }
        return ROOT.equalsIgnoreCase(logger) ? ROOT : logger;
    }

    /**
     * Reads the requested level from the body.
     *
     * @return the level, or {@code null} for {@code reset}.
     * @throws IllegalArgumentException if the level is missing or not accepted.
     */
    private static LogLevel parseLevel(final Map<String, Object> body)
    {
        Object raw = body == null ? null : body.get(KEY_LEVEL);
        String value = raw instanceof String ? ((String) raw).trim() : "";
        if (value.isEmpty())
        {
            throw new IllegalArgumentException("'" + KEY_LEVEL + "' is required and must be one of " + ACCEPTED_LEVELS);
        }
        if (RESET.equalsIgnoreCase(value))
        {
            return null;
        }
        LogLevel level = SETTABLE_LEVELS.get(value.toUpperCase(Locale.ROOT));
        if (level == null)
        {
            throw new IllegalArgumentException("Invalid level '" + value + "', must be one of " + ACCEPTED_LEVELS);
        }
        return level;
    }

    /**
     * Describes a logger. A logger that was never created has no configuration of its own, so its effective level is
     * that of its nearest existing ancestor (ultimately ROOT), which is what it would inherit once created.
     */
    private Map<String, Object> describeWithInheritance(final String name)
    {
        LoggerConfiguration configuration = myLoggingSystem.getLoggerConfiguration(name);
        if (configuration != null)
        {
            return describe(name, configuration);
        }

        String ancestor = name;
        LoggerConfiguration inherited = null;
        while (inherited == null)
        {
            int lastDot = ancestor.lastIndexOf('.');
            ancestor = lastDot > 0 ? ancestor.substring(0, lastDot) : ROOT;
            inherited = myLoggingSystem.getLoggerConfiguration(ancestor);
        }
        Map<String, Object> described = new LinkedHashMap<>();
        described.put("logger", name);
        described.put("configured_level", null);
        described.put("effective_level", levelName(inherited.getEffectiveLevel()));
        return described;
    }

    private static Map<String, Object> describe(final String name, final LoggerConfiguration configuration)
    {
        Map<String, Object> described = new LinkedHashMap<>();
        described.put("logger", name);
        described.put("configured_level", levelName(configuration.getConfiguredLevel()));
        described.put("effective_level", levelName(configuration.getEffectiveLevel()));
        return described;
    }

    private static String levelName(final LogLevel level)
    {
        return level == null ? null : level.name();
    }

    private static ResponseEntity<Map<String, Object>> badRequest(final String message)
    {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("error", message);
        return ResponseEntity.badRequest().body(error);
    }
}
