/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.application.AbstractRepairConfigurationProvider;
import com.ericsson.bss.cassandra.ecchronos.application.FileBasedRepairConfiguration;
import com.ericsson.bss.cassandra.ecchronos.application.config.Interval;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairLockType;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.context.ApplicationContext;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/** Global repair configuration that applies to all tables by default. */
public class GlobalRepairConfig extends RepairConfig
{
    private static final int THIRTY_DAYS = 30;

    private Class<? extends AbstractRepairConfigurationProvider> myRepairConfigurationClass =
            FileBasedRepairConfiguration.class;
    private RepairLockType myRepairLockType = RepairLockType.VNODE;
    private Interval myRepairHistoryLookback = new Interval(THIRTY_DAYS, TimeUnit.DAYS);
    private RepairHistory myRepairHistory = new RepairHistory();

    /** Default constructor. */
    public GlobalRepairConfig()
    {
    }

    /**
     * Returns the repair configuration class.
     * @return the repair configuration class
     */
    @JsonProperty("provider")
    public final Class<? extends AbstractRepairConfigurationProvider> getRepairConfigurationClass()
    {
        return myRepairConfigurationClass;
    }

    /**
     * Sets the repair configuration class.
     * @param repairConfigurationClass the repair configuration class
     * @throws NoSuchMethodException if the method is not found
     */
    @JsonProperty("provider")
    public final void setRepairConfigurationClass(final Class<? extends AbstractRepairConfigurationProvider>
            repairConfigurationClass) throws NoSuchMethodException
    {
        myRepairConfigurationClass.getDeclaredConstructor(ApplicationContext.class);

        myRepairConfigurationClass = repairConfigurationClass;
    }

    /**
     * Returns the repair lock type.
     * @return the repair lock type
     */
    @JsonProperty("lock_type")
    public final RepairLockType getRepairLockType()
    {
        return myRepairLockType;
    }

    /**
     * Sets the repair lock type.
     * @param repairLockType the repair lock type
     */
    @JsonProperty("lock_type")
    public final void setRepairLockType(final String repairLockType)
    {
        myRepairLockType = RepairLockType.valueOf(repairLockType.toUpperCase(Locale.US));
    }

    /**
     * Returns the repair history lookback.
     * @return the repair history lookback
     */
    @JsonProperty("history_lookback")
    public final Interval getRepairHistoryLookback()
    {
        return myRepairHistoryLookback;
    }

    /**
     * Sets the repair history lookback.
     * @param repairHistoryLookback the repair history lookback
     */
    @JsonProperty("history_lookback")
    public final void setRepairHistoryLookback(final Interval repairHistoryLookback)
    {
        myRepairHistoryLookback = repairHistoryLookback;
    }

    /**
     * Returns the repair history.
     * @return the repair history
     */
    @JsonProperty("history")
    public final RepairHistory getRepairHistory()
    {
        return myRepairHistory;
    }

    /**
     * Sets the repair history.
     * @param repairHistory the repair history
     */
    @JsonProperty("history")
    public final void setRepairHistory(final RepairHistory repairHistory)
    {
        myRepairHistory = repairHistory;
    }
}
