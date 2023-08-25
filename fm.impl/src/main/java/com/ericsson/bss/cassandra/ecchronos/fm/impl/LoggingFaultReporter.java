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
package com.ericsson.bss.cassandra.ecchronos.fm.impl;

import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bss.cassandra.ecchronos.fm.RepairFaultReporter;

@Component(service = RepairFaultReporter.class)
public class LoggingFaultReporter implements RepairFaultReporter
{
    private static final Logger LOG = LoggerFactory.getLogger(LoggingFaultReporter.class);
    Map<Integer, FaultCode> alarms = new HashMap<>();

    @Override
    public void raise(FaultCode faultCode, Map<String, Object> data)
    {
        FaultCode oldCode = alarms.put(data.hashCode(), faultCode);
        if (oldCode == null || (oldCode == FaultCode.REPAIR_WARNING && faultCode == FaultCode.REPAIR_ERROR))
        {
            LOG.error("Raising alarm: {} - {}", faultCode, data);
        }
    }

    @Override
    public void cease(FaultCode faultCode, Map<String, Object> data)
    {
        FaultCode code = alarms.get(data.hashCode());
        if (code != null)
        {
            LOG.info("Ceasing alarm: {} - {}", code , data);
            alarms.remove(data.hashCode(), code);
        }
    }
}
