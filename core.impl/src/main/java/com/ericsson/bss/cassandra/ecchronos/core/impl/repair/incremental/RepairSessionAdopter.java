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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair.incremental;

import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairSessionInfo;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers and awaits an already-started incremental (consistent) repair session after a lost {@code repairAsync}
 * response, so a repair that Cassandra actually started is adopted rather than re-triggered or reported as failed.
 * See issue #1892.
 * <p>
 * Only a session whose {@code STARTED} is at/after the request time (less a clock-skew tolerance) is adopted, so a
 * retained older {@code FINALIZED} session for the same table is never mistaken for the just-started one. If the
 * started session has not yet appeared, discovery is retried for a short window before giving up ({@link Result#NOT_ADOPTED},
 * a safe retry).
 */
final class RepairSessionAdopter
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairSessionAdopter.class);

    private static final String STATE_FINALIZED = "FINALIZED";
    private static final String STATE_FAILED = "FAILED";

    /**
     * The outcome of an adoption attempt.
     */
    enum Result
    {
        /** A matching session was found and awaited to successful completion. */
        ADOPTED_SUCCESS,
        /** A matching session was found and awaited, but it ended in failure. */
        ADOPTED_FAILED,
        /** No matching session could be found/awaited within the wait window. */
        NOT_ADOPTED
    }

    private final DistributedJmxProxy myProxy;
    private final UUID myNodeId;
    private final String myQualifiedTable;
    private final long myMinStarted;
    private final long myAdoptionWaitMs;
    private final long myDiscoveryWaitMs;
    private final long myPollIntervalMs;

    // CHECKSTYLE:OFF ParameterNumber
    RepairSessionAdopter(final DistributedJmxProxy proxy, final UUID nodeId, final String qualifiedTable,
            final long minStarted, final long adoptionWaitMs, final long discoveryWaitMs, final long pollIntervalMs)
    {
        myProxy = proxy;
        myNodeId = nodeId;
        myQualifiedTable = qualifiedTable;
        myMinStarted = minStarted;
        myAdoptionWaitMs = adoptionWaitMs;
        myDiscoveryWaitMs = discoveryWaitMs;
        myPollIntervalMs = pollIntervalMs;
    }
    // CHECKSTYLE:ON ParameterNumber

    /**
     * Discover the just-started session and await its terminal state.
     *
     * @return the adoption result.
     */
    Result adopt()
    {
        String sessionId = discover();
        if (sessionId == null)
        {
            return Result.NOT_ADOPTED;
        }
        LOG.info("Adopting already-started incremental repair session {} for {} on node {} after a lost "
                + "repairAsync response", sessionId, myQualifiedTable, myNodeId);
        return await(sessionId);
    }

    /**
     * Poll for the just-started session to appear, up to the discovery window.
     *
     * @return the discovered session id, or {@code null} if none appeared in time.
     */
    private String discover()
    {
        long discoveryDeadline = System.currentTimeMillis() + myDiscoveryWaitMs;
        while (true)
        {
            String sessionId = findStartedSession();
            if (sessionId != null)
            {
                return sessionId;
            }
            if (System.currentTimeMillis() >= discoveryDeadline || !sleep())
            {
                LOG.warn("Could not discover a started incremental repair session for {} on node {} "
                        + "(started at/after {}s) after a lost repairAsync response",
                        myQualifiedTable, myNodeId, myMinStarted);
                return null;
            }
        }
    }

    /**
     * Await the discovered session's terminal state, up to the adoption window.
     *
     * @param sessionId the session to await.
     * @return the adoption result.
     */
    private Result await(final String sessionId)
    {
        long deadline = System.currentTimeMillis() + myAdoptionWaitMs;
        while (System.currentTimeMillis() < deadline)
        {
            Result terminal = terminalResult(findSessionState(sessionId));
            if (terminal != null)
            {
                return terminal;
            }
            if (!sleep())
            {
                return Result.NOT_ADOPTED;
            }
        }
        LOG.warn("Timed out awaiting adopted repair session {} for {} on node {} after {} ms", sessionId,
                myQualifiedTable, myNodeId, myAdoptionWaitMs);
        return Result.NOT_ADOPTED;
    }

    /**
     * Find the most recently started session covering the table and started at/after the minimum. A session already
     * in a terminal state is still eligible (the repair may have finished between the lost response and this lookup);
     * its outcome is resolved by {@link #terminalResult(String)}.
     *
     * @return the matching session id, or {@code null}.
     */
    private String findStartedSession()
    {
        String bestSessionId = null;
        long bestStarted = Long.MIN_VALUE;
        for (Map<String, String> session : myProxy.getRepairSessions(myNodeId))
        {
            String tables = session.get(RepairSessionInfo.TABLES);
            if (tables == null || !containsTable(tables))
            {
                continue;
            }
            long started = parseEpochSeconds(session.get(RepairSessionInfo.STARTED));
            if (started >= myMinStarted && started >= bestStarted)
            {
                bestStarted = started;
                bestSessionId = session.get(RepairSessionInfo.SESSION_ID);
            }
        }
        return bestSessionId;
    }

    private String findSessionState(final String sessionId)
    {
        for (Map<String, String> session : myProxy.getRepairSessions(myNodeId))
        {
            if (sessionId.equals(session.get(RepairSessionInfo.SESSION_ID)))
            {
                return session.get(RepairSessionInfo.STATE);
            }
        }
        return null;
    }

    private boolean containsTable(final String tablesCsv)
    {
        for (String entry : tablesCsv.split(","))
        {
            if (entry.trim().equals(myQualifiedTable))
            {
                return true;
            }
        }
        return false;
    }

    private boolean sleep()
    {
        try
        {
            Thread.sleep(myPollIntervalMs);
            return true;
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Result terminalResult(final String state)
    {
        if (STATE_FINALIZED.equals(state))
        {
            return Result.ADOPTED_SUCCESS;
        }
        if (STATE_FAILED.equals(state))
        {
            return Result.ADOPTED_FAILED;
        }
        return null;
    }

    private static long parseEpochSeconds(final String value)
    {
        if (value == null)
        {
            return Long.MIN_VALUE;
        }
        try
        {
            return Long.parseLong(value.trim());
        }
        catch (NumberFormatException e)
        {
            return Long.MIN_VALUE;
        }
    }
}
