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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;

@RestController
public final class ConfigManagementRESTImpl
{
    private static final String KEY_SESSION_WINDOW = "session_window_ms";
    private static final String KEY_COOLDOWN = "cooldown_ms";
    private static final String KEY_LOCKS_PER_RESOURCE = "locks_per_resource";
    private static final int MIN_LOCKS_PER_RESOURCE = 1;

    private final ScheduleManager myScheduleManager;

    @Autowired
    public ConfigManagementRESTImpl(final ScheduleManager scheduleManager)
    {
        myScheduleManager = scheduleManager;
    }

    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/v2/config", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getConfig()
    {
        return ResponseEntity.ok(buildResponse());
    }

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
        if (body.containsKey(KEY_SESSION_WINDOW))
        {
            myScheduleManager.setSessionWindowInMs(((Number) body.get(KEY_SESSION_WINDOW)).longValue());
        }
        if (body.containsKey(KEY_COOLDOWN))
        {
            myScheduleManager.setCooldownInMs(((Number) body.get(KEY_COOLDOWN)).longValue());
        }
        if (body.containsKey(KEY_LOCKS_PER_RESOURCE))
        {
            myScheduleManager.setLocksPerResource(((Number) body.get(KEY_LOCKS_PER_RESOURCE)).intValue());
        }
    }

    private Map<String, Object> buildResponse()
    {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(KEY_SESSION_WINDOW, myScheduleManager.getSessionWindowInMs());
        config.put(KEY_COOLDOWN, myScheduleManager.getCooldownInMs());
        config.put(KEY_LOCKS_PER_RESOURCE, myScheduleManager.getLocksPerResource());
        return config;
    }
}
