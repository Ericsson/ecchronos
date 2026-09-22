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

import com.ericsson.bss.cassandra.ecchronos.core.repair.scheduler.ScheduleManager;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.HungRepairSessionRecovery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;

/**
 * REST controller for managing ecChronos scheduling configuration such as session window,
 * cooldown, and locks per resource.
 */
@RestController
public final class ConfigManagementRESTImpl
{
    private static final String KEY_SESSION_WINDOW = "session_window_ms";
    private static final String KEY_COOLDOWN = "cooldown_ms";
    private static final String KEY_LOCKS_PER_RESOURCE = "locks_per_resource";
    private static final String KEY_MAX_WAIT_TIME = "max_wait_time_minutes";
    private static final String KEY_HUNG_REPAIR_ENABLED = "hung_repair_recovery_enabled";
    private static final String KEY_HUNG_REPAIR_THRESHOLD = "hung_repair_stall_threshold_ms";
    private static final String KEY_HUNG_REPAIR_BYPASS = "hung_repair_bypass_coordinator_check";
    private static final String KEY_HUNG_REPAIR_FORCE = "hung_repair_force";
    private static final int MIN_LOCKS_PER_RESOURCE = 1;
    private static final int MIN_MAX_WAIT_TIME = 1;
    private static final long MIN_HUNG_REPAIR_THRESHOLD = 1;

    private final ScheduleManager myScheduleManager;
    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final HungRepairSessionRecovery myHungRepairSessionRecovery;

    /**
     * Constructs the configuration management REST controller.
     *
     * @param scheduleManager the schedule manager providing configuration access.
     * @param jmxProxyFactory the JMX proxy factory providing the repair max wait time.
     * @param hungRepairSessionRecovery the hung repair session recovery component.
     */
    @Autowired
    public ConfigManagementRESTImpl(final ScheduleManager scheduleManager,
            final DistributedJmxProxyFactory jmxProxyFactory,
            final HungRepairSessionRecovery hungRepairSessionRecovery)
    {
        myScheduleManager = scheduleManager;
        myJmxProxyFactory = jmxProxyFactory;
        myHungRepairSessionRecovery = hungRepairSessionRecovery;
    }

    /**
     * Retrieves the current scheduling configuration.
     *
     * @return a response containing the configuration as a map of key-value pairs.
     */
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/v2/config", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getConfig()
    {
        return ResponseEntity.ok(buildResponse());
    }

    /**
     * Partially updates the scheduling configuration with the provided values.
     *
     * @param body a map containing the configuration keys and new values to apply.
     * @return a response containing the updated configuration, or a bad request on invalid input.
     */
    @PatchMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/v2/config", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> patchConfig(@RequestBody final Map<String, Object> body)
    {
        try
        {
            validatePatchBody(body);
            applyPatchBody(body);
        }
        catch (IllegalArgumentException e)
        {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
        return ResponseEntity.ok(buildResponse());
    }

    private void validatePatchBody(final Map<String, Object> body)
    {
        validateMin(body, KEY_SESSION_WINDOW, 1, "session_window must be > 0");
        validateMin(body, KEY_COOLDOWN, 0, "cooldown must be >= 0");
        validateMin(body, KEY_LOCKS_PER_RESOURCE, MIN_LOCKS_PER_RESOURCE, "locks_per_resource must be >= 1");
        validateMin(body, KEY_MAX_WAIT_TIME, MIN_MAX_WAIT_TIME, "max_wait_time_minutes must be > 0");
        validateMin(body, KEY_HUNG_REPAIR_THRESHOLD, MIN_HUNG_REPAIR_THRESHOLD,
                "hung_repair_stall_threshold_ms must be > 0");
        validateBoolean(body, KEY_HUNG_REPAIR_ENABLED, "hung_repair_recovery_enabled must be a boolean");
        validateBoolean(body, KEY_HUNG_REPAIR_BYPASS, "hung_repair_bypass_coordinator_check must be a boolean");
        validateBoolean(body, KEY_HUNG_REPAIR_FORCE, "hung_repair_force must be a boolean");
    }

    private void validateBoolean(final Map<String, Object> body, final String key, final String message)
    {
        if (body.containsKey(key) && !(body.get(key) instanceof Boolean))
        {
            throw new IllegalArgumentException(message);
        }
    }

    private void validateMin(final Map<String, Object> body, final String key, final long min, final String message)
    {
        if (body.containsKey(key) && ((Number) body.get(key)).longValue() < min)
        {
            throw new IllegalArgumentException(message);
        }
    }

    private void applyPatchBody(final Map<String, Object> body)
    {
        applyLong(body, KEY_SESSION_WINDOW, myScheduleManager::setSessionWindowInMs);
        applyLong(body, KEY_COOLDOWN, myScheduleManager::setCooldownInMs);
        applyInt(body, KEY_LOCKS_PER_RESOURCE, myScheduleManager::setLocksPerResource);
        applyInt(body, KEY_MAX_WAIT_TIME, myJmxProxyFactory::setMaxWaitTimeInMinutes);
        applyBoolean(body, KEY_HUNG_REPAIR_ENABLED, myHungRepairSessionRecovery::setEnabled);
        applyLong(body, KEY_HUNG_REPAIR_THRESHOLD, myHungRepairSessionRecovery::setStallThresholdMs);
        applyBoolean(body, KEY_HUNG_REPAIR_BYPASS, myHungRepairSessionRecovery::setBypassCoordinatorCheck);
        applyBoolean(body, KEY_HUNG_REPAIR_FORCE, myHungRepairSessionRecovery::setForce);
    }

    private void applyLong(final Map<String, Object> body, final String key, final LongConsumer setter)
    {
        if (body.containsKey(key))
        {
            setter.accept(((Number) body.get(key)).longValue());
        }
    }

    private void applyInt(final Map<String, Object> body, final String key, final IntConsumer setter)
    {
        if (body.containsKey(key))
        {
            setter.accept(((Number) body.get(key)).intValue());
        }
    }

    private void applyBoolean(final Map<String, Object> body, final String key, final Consumer<Boolean> setter)
    {
        if (body.containsKey(key))
        {
            setter.accept((Boolean) body.get(key));
        }
    }

    private Map<String, Object> buildResponse()
    {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(KEY_SESSION_WINDOW, myScheduleManager.getSessionWindowInMs());
        config.put(KEY_COOLDOWN, myScheduleManager.getCooldownInMs());
        config.put(KEY_LOCKS_PER_RESOURCE, myScheduleManager.getLocksPerResource());
        config.put(KEY_MAX_WAIT_TIME, myJmxProxyFactory.getMaxWaitTimeInMinutes());
        config.put(KEY_HUNG_REPAIR_ENABLED, myHungRepairSessionRecovery.isEnabled());
        config.put(KEY_HUNG_REPAIR_THRESHOLD, myHungRepairSessionRecovery.getStallThresholdMs());
        config.put(KEY_HUNG_REPAIR_BYPASS, myHungRepairSessionRecovery.isBypassCoordinatorCheck());
        config.put(KEY_HUNG_REPAIR_FORCE, myHungRepairSessionRecovery.isForce());
        return config;
    }
}
