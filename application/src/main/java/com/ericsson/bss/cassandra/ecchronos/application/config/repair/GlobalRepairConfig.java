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
import com.ericsson.bss.cassandra.ecchronos.core.repair.config.RepairConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.TimeUnit;

/** Global repair configuration that applies to all tables by default. */
public class GlobalRepairConfig extends RepairConfig
{
    private static final Logger LOG = LoggerFactory.getLogger(GlobalRepairConfig.class);
    private static final int THIRTY_DAYS = 30;

    private Class<? extends AbstractRepairConfigurationProvider> myRepairConfigurationClass =
            FileBasedRepairConfiguration.class;
    private Interval myRepairHistoryLookback = new Interval(THIRTY_DAYS, TimeUnit.DAYS);
    private RepairHistory myRepairHistory = new RepairHistory();
    private RepairLockType myRepairLockType = RepairLockType.VNODE;
    private static final int DEFAULT_HISTORY_LOOKBACK_INTERVALS = 2;
    private int myHistoryLookbackIntervals = DEFAULT_HISTORY_LOOKBACK_INTERVALS;

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
     * Returns the number of repair intervals of history to read back when priming repair state at cold start
     * (0 disables history-based priming). See issue #1952.
     * @return the history lookback, in repair intervals
     */
    @JsonProperty("history_lookback_intervals")
    @Override
    public final int getHistoryLookbackIntervals()
    {
        return myHistoryLookbackIntervals;
    }

    /**
     * Sets the number of repair intervals of history to read back when priming repair state at cold start.
     * @param historyLookbackIntervals the history lookback, in repair intervals
     */
    @JsonProperty("history_lookback_intervals")
    public final void setHistoryLookbackIntervals(final int historyLookbackIntervals)
    {
        myHistoryLookbackIntervals = historyLookbackIntervals;
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

    /**
     * {@inheritDoc}
     * <p>
     * Additionally warns at startup if {@code initial_delay} is shorter than {@code repair_interval}. With a small
     * (or {@code 0}) history_lookback_intervals, a node that cannot find recent history begins repairing almost
     * immediately after the initial delay, so a short initial delay interacts badly with the reduced readback.
     * See issue #1952.
     */
    @Override
    public RepairConfiguration asRepairConfiguration()
    {
        long initialDelayMs = getInitialDelay().getInterval(TimeUnit.MILLISECONDS);
        long repairIntervalMs = getRepairInterval().getInterval(TimeUnit.MILLISECONDS);
        if (initialDelayMs < repairIntervalMs)
        {
            LOG.warn("initial_delay ({} ms) is shorter than repair_interval ({} ms); after the initial delay a node "
                    + "with no recent repair history will begin repairing almost immediately, which may be "
                    + "undesirable with a small history_lookback_intervals.", initialDelayMs, repairIntervalMs);
        }
        return super.asRepairConfiguration();
    }
}

