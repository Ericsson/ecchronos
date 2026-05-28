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
package com.ericsson.bss.cassandra.ecchronos.core.impl.jmx;

import org.jolokia.json.JSONObject;

import javax.management.openmbean.CompositeData;
import java.util.List;

/**
 * Parses repair stats results from JMX/Jolokia into maxRepaired values.
 */
final class RepairStatsParser
{
    private RepairStatsParser()
    {
    }

    @SuppressWarnings("unchecked")
    static long extractMaxRepairedValue(final Object result, final boolean jolokiaEnabled)
    {
        if (!(result instanceof List))
        {
            return 0;
        }
        List<?> resultList = (List<?>) result;
        if (jolokiaEnabled)
        {
            return extractFromJolokiaResult(resultList);
        }
        return extractFromCompositeDataResult((List<CompositeData>) resultList);
    }

    private static long extractFromJolokiaResult(final List<?> resultList)
    {
        for (Object item : resultList)
        {
            if (item instanceof JSONObject)
            {
                long value = extractFromJsonObject((JSONObject) item);
                if (value > 0)
                {
                    return value;
                }
            }
            else if (item instanceof CompositeData)
            {
                return extractFromCompositeData((CompositeData) item);
            }
        }
        return 0;
    }

    private static long extractFromJsonObject(final JSONObject jsonObj)
    {
        Object maxRepaired = jsonObj.get("maxRepaired");
        return (maxRepaired instanceof Number) ? ((Number) maxRepaired).longValue() : 0;
    }

    private static long extractFromCompositeDataResult(final List<CompositeData> compositeDatas)
    {
        for (CompositeData data : compositeDatas)
        {
            return extractFromCompositeData(data);
        }
        return 0;
    }

    private static long extractFromCompositeData(final CompositeData data)
    {
        return (long) data.getAll(new String[]{"maxRepaired"})[0];
    }
}
