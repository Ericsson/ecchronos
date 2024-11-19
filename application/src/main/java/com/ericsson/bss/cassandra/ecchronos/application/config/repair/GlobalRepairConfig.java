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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import com.ericsson.bss.cassandra.ecchronos.application.spring.AbstractRepairConfigurationProvider;

import com.ericsson.bss.cassandra.ecchronos.core.impl.locks.RepairLockType;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Locale;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.TimeUnit;

public class GlobalRepairConfig extends RepairConfig
{
    private static final int THIRTY_DAYS = 30;

    private Class<? extends AbstractRepairConfigurationProvider> myRepairConfigurationClass =
            FileBasedRepairConfiguration.class;
    private Interval myRepairHistoryLookback = new Interval(THIRTY_DAYS, TimeUnit.DAYS);
    private RepairHistory myRepairHistory = new RepairHistory();
    private RepairLockType myRepairLockType = RepairLockType.VNODE;

    @JsonProperty("provider")
    public final Class<? extends AbstractRepairConfigurationProvider> getRepairConfigurationClass()
    {
        return myRepairConfigurationClass;
    }

    @JsonProperty("provider")
    public final void setRepairConfigurationClass(final Class<? extends AbstractRepairConfigurationProvider>
            repairConfigurationClass) throws NoSuchMethodException
    {
        myRepairConfigurationClass.getDeclaredConstructor(ApplicationContext.class);

        myRepairConfigurationClass = repairConfigurationClass;
    }

    @JsonProperty("lock_type")
    public final RepairLockType getRepairLockType()
    {
        return myRepairLockType;
    }

    @JsonProperty("lock_type")
    public final void setRepairLockType(final String repairLockType)
    {
        myRepairLockType = RepairLockType.valueOf(repairLockType.toUpperCase(Locale.US));
    }

    @JsonProperty("history_lookback")
    public final Interval getRepairHistoryLookback()
    {
        return myRepairHistoryLookback;
    }

    @JsonProperty("history_lookback")
    public final void setRepairHistoryLookback(final Interval repairHistoryLookback)
    {
        myRepairHistoryLookback = repairHistoryLookback;
    }

    @JsonProperty("history")
    public final RepairHistory getRepairHistory()
    {
        return myRepairHistory;
    }

    @JsonProperty("history")
    public final void setRepairHistory(final RepairHistory repairHistory)
    {
        myRepairHistory = repairHistory;
    }
}

