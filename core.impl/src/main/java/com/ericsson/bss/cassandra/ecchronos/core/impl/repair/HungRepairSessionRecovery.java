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
package com.ericsson.bss.cassandra.ecchronos.core.impl.repair;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Automatically recovers from hung Cassandra incremental repair sessions (issue #1825).
 * <p>
 * When an incremental repair session hangs it stays in the {@code REPAIRING} state holding SSTables, which makes
 * subsequent incremental repairs of the same table abort in the prepare phase. This component periodically scans, for
 * each managed node, that node's own repair sessions (via the {@code RepairService} MBean, the JMX surface behind
 * {@code nodetool repair_admin list}) and cancels — coordinator-only, session-scoped, via {@code failSession} — any
 * session that has been {@code REPAIRING} without activity for longer than a configurable stall threshold.
 * <p>
 * The component is <strong>disabled by default</strong> and the enabled flag, the stall threshold, the
 * coordinator-bypass flag and the force flag are runtime-tunable (in-memory) via validated setters. By default it
 * never uses the cluster-wide {@code forceTerminateAllRepairSessions} (see #1815); only the specific hung session is
 * failed, on its own coordinator. When {@code bypassCoordinatorCheck} is enabled the coordinator match is skipped and
 * the {@code failSession} command for a hung session is sent to every managed node that reports it, not only the
 * coordinator. The {@code force} flag (default {@code false}) is passed to {@code failSession} for every request
 * regardless of coordinator status.
 */
public final class HungRepairSessionRecovery implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(HungRepairSessionRecovery.class);

    // Keys and value semantics come from Cassandra's org.apache.cassandra.repair.consistent.LocalSessionInfo:
    //   SESSION_ID   = session.sessionID.toString()
    //   STATE        = ConsistentSession.State enum name, e.g. "REPAIRING"
    //   LAST_UPDATE  = absolute epoch SECONDS of the last session update (session.getLastUpdate(), nowInSeconds())
    //   COORDINATOR  = InetAddressAndPort.toString() of the coordinator, e.g. "/127.0.0.1:7000" or "/[::1]:7000"
    private static final String SESSION_KEY_ID = "SESSION_ID";
    private static final String SESSION_KEY_STATE = "STATE";
    private static final String SESSION_KEY_LAST_UPDATE = "LAST_UPDATE";
    private static final String SESSION_KEY_COORDINATOR = "COORDINATOR";
    private static final String STATE_REPAIRING = "REPAIRING";
    private static final long SINGLE_COLON = 1L;
    private static final long DEFAULT_SCAN_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long DEFAULT_STALL_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(30);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;
    private final ScheduledExecutorService myScheduler;
    private final long myScanIntervalMs;
    private final LongSupplier myClock;

    private volatile boolean myEnabled;
    private volatile long myStallThresholdMs;
    private volatile boolean myBypassCoordinatorCheck;
    private volatile boolean myForce;

    /**
     * Constructs a recovery component with default scan interval and stall threshold, disabled.
     *
     * @param jmxProxyFactory the factory used to obtain JMX proxies.
     * @param nativeConnectionProvider the provider of managed nodes.
     */
    public HungRepairSessionRecovery(final DistributedJmxProxyFactory jmxProxyFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        this(jmxProxyFactory, nativeConnectionProvider, false, DEFAULT_STALL_THRESHOLD_MS, DEFAULT_SCAN_INTERVAL_MS,
                false, false);
    }

    /**
     * Constructs a recovery component with explicit initial configuration.
     *
     * @param jmxProxyFactory the factory used to obtain JMX proxies.
     * @param nativeConnectionProvider the provider of managed nodes.
     * @param enabled whether recovery is initially enabled.
     * @param stallThresholdMs staleness in milliseconds before a REPAIRING session is cancelled. Must be > 0.
     * @param scanIntervalMs how often to scan for hung sessions, in milliseconds.
     */
    public HungRepairSessionRecovery(final DistributedJmxProxyFactory jmxProxyFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final boolean enabled,
            final long stallThresholdMs,
            final long scanIntervalMs)
    {
        this(jmxProxyFactory, nativeConnectionProvider, enabled, stallThresholdMs, scanIntervalMs, false, false);
    }

    /**
     * Constructs a recovery component with explicit initial configuration.
     *
     * @param jmxProxyFactory the factory used to obtain JMX proxies.
     * @param nativeConnectionProvider the provider of managed nodes.
     * @param enabled whether recovery is initially enabled.
     * @param stallThresholdMs staleness in milliseconds before a REPAIRING session is cancelled. Must be > 0.
     * @param scanIntervalMs how often to scan for hung sessions, in milliseconds.
     * @param bypassCoordinatorCheck when true, skip the coordinator match and send the fail command to every
     *         managed node instead of only the coordinator.
     * @param force whether {@code failSession} is invoked with force=true for all requests.
     */
    public HungRepairSessionRecovery(final DistributedJmxProxyFactory jmxProxyFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final boolean enabled,
            final long stallThresholdMs,
            final long scanIntervalMs,
            final boolean bypassCoordinatorCheck,
            final boolean force)
    {
        this(jmxProxyFactory, nativeConnectionProvider, enabled, stallThresholdMs, scanIntervalMs,
                bypassCoordinatorCheck, force, System::currentTimeMillis);
    }

    @VisibleForTesting
    HungRepairSessionRecovery(final DistributedJmxProxyFactory jmxProxyFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider,
            final boolean enabled,
            final long stallThresholdMs,
            final long scanIntervalMs,
            final boolean bypassCoordinatorCheck,
            final boolean force,
            final LongSupplier clock)
    {
        myJmxProxyFactory = jmxProxyFactory;
        myNativeConnectionProvider = nativeConnectionProvider;
        myEnabled = enabled;
        myStallThresholdMs = validateThreshold(stallThresholdMs);
        myScanIntervalMs = scanIntervalMs;
        myBypassCoordinatorCheck = bypassCoordinatorCheck;
        myForce = force;
        myClock = clock;
        myScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("HungRepairRecovery-%d").setDaemon(true).build());
    }

    /**
     * Start the periodic scan.
     */
    public void start()
    {
        myScheduler.scheduleWithFixedDelay(this::scan, myScanIntervalMs, myScanIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("Hung repair session recovery started (enabled={}, stallThreshold={} ms, scanInterval={} ms, "
                + "bypassCoordinatorCheck={}, force={})", myEnabled, myStallThresholdMs, myScanIntervalMs,
                myBypassCoordinatorCheck, myForce);
    }

    /**
     * Whether recovery is currently enabled.
     *
     * @return true if enabled.
     */
    public boolean isEnabled()
    {
        return myEnabled;
    }

    /**
     * Enable or disable recovery at runtime.
     *
     * @param enabled the new enabled state.
     */
    public void setEnabled(final boolean enabled)
    {
        myEnabled = enabled;
        LOG.info("Hung repair session recovery enabled set to {}", enabled);
    }

    /**
     * Get the current stall threshold in milliseconds.
     *
     * @return the stall threshold in milliseconds.
     */
    public long getStallThresholdMs()
    {
        return myStallThresholdMs;
    }

    /**
     * Set the stall threshold at runtime.
     *
     * @param stallThresholdMs the new stall threshold in milliseconds. Must be > 0.
     */
    public void setStallThresholdMs(final long stallThresholdMs)
    {
        myStallThresholdMs = validateThreshold(stallThresholdMs);
        LOG.info("Hung repair session stall threshold set to {} ms", myStallThresholdMs);
    }

    /**
     * Whether the coordinator check is bypassed. When true, hung sessions are failed on every managed node instead
     * of only their coordinator.
     *
     * @return true if the coordinator check is bypassed.
     */
    public boolean isBypassCoordinatorCheck()
    {
        return myBypassCoordinatorCheck;
    }

    /**
     * Enable or disable bypassing the coordinator check at runtime. When enabled, the {@code failSession} command
     * for a hung session is sent to every managed node rather than only the coordinator.
     *
     * @param bypassCoordinatorCheck the new bypass state.
     */
    public void setBypassCoordinatorCheck(final boolean bypassCoordinatorCheck)
    {
        myBypassCoordinatorCheck = bypassCoordinatorCheck;
        LOG.info("Hung repair session recovery bypassCoordinatorCheck set to {}", bypassCoordinatorCheck);
    }

    /**
     * Whether {@code failSession} is invoked with force=true. This value is used for all requests regardless of
     * whether they target the coordinator.
     *
     * @return true if force is enabled.
     */
    public boolean isForce()
    {
        return myForce;
    }

    /**
     * Enable or disable force at runtime. When enabled, {@code failSession} is invoked with force=true for all
     * requests, regardless of whether they target the coordinator.
     *
     * @param force the new force state.
     */
    public void setForce(final boolean force)
    {
        myForce = force;
        LOG.info("Hung repair session recovery force set to {}", force);
    }

    private static long validateThreshold(final long stallThresholdMs)
    {
        if (stallThresholdMs <= 0)
        {
            throw new IllegalArgumentException("hung repair stall threshold must be > 0");
        }
        return stallThresholdMs;
    }

    @VisibleForTesting
    void scan()
    {
        if (!myEnabled)
        {
            return;
        }
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            for (UUID nodeID : myNativeConnectionProvider.getNodes().keySet())
            {
                recoverNode(proxy, nodeID);
            }
        }
        catch (IOException e)
        {
            LOG.warn("Hung repair recovery scan could not obtain a JMX proxy", e);
        }
        catch (Exception e)
        {
            LOG.warn("Unexpected error during hung repair recovery scan", e);
        }
    }

    private void recoverNode(final DistributedJmxProxy proxy, final UUID nodeID)
    {
        Node node = myNativeConnectionProvider.getNodes().get(nodeID);
        if (node == null)
        {
            return;
        }
        InetAddress coordinatorAddress = new DriverNode(node).getPublicAddress();
        // Scan this node's own sessions. By default only sessions this node coordinates are cancelled, on the node
        // itself. When the coordinator check is bypassed, every hung session is cancelled on every managed node.
        for (Map<String, String> session : proxy.getRepairSessions(nodeID))
        {
            if (!isHung(session))
            {
                continue;
            }
            boolean bypass = myBypassCoordinatorCheck;
            boolean coordinator = isCoordinatedBy(session, coordinatorAddress);
            if (!bypass && !coordinator)
            {
                continue;
            }
            String sessionId = session.get(SESSION_KEY_ID);
            if (coordinator)
            {
                LOG.warn("Cancelling hung repair session {} on node {} (state={}, lastUpdate={}, threshold={}ms)",
                        sessionId, nodeID, session.get(SESSION_KEY_STATE),
                        session.get(SESSION_KEY_LAST_UPDATE), myStallThresholdMs);
                failSession(proxy, nodeID, sessionId, false);
            }
            else if (bypass)
            {
                LOG.warn("Cancelling hung repair session {} on non-coordinator node (found on node {}, state={}, "
                        + "lastUpdate={}, threshold={}ms, coordinator check bypassed)", sessionId, nodeID,
                        session.get(SESSION_KEY_STATE), session.get(SESSION_KEY_LAST_UPDATE), myStallThresholdMs);
                failSession(proxy, nodeID, sessionId, true);
            }
        }
    }

    private void failSession(final DistributedJmxProxy proxy, final UUID nodeID, final String sessionId,
            final boolean debugLog)
    {
        try
        {
            // The force flag is applied uniformly to every request, regardless of coordinator status.
            proxy.failRepairSession(nodeID, sessionId, myForce);
        }
        catch (RuntimeException e)
        {
            if (debugLog)
            {
                LOG.debug("Unable to fail repair session {} on node {}", sessionId, nodeID, e);
            }
            else
            {
                LOG.error("Unable to fail repair session {} on node {}", sessionId, nodeID, e);
            }
        }
    }

    private boolean isCoordinatedBy(final Map<String, String> session, final InetAddress nodeAddress)
    {
        String host = extractHost(session.get(SESSION_KEY_COORDINATOR));
        if (host == null)
        {
            return false;
        }
        try
        {
            // Compare parsed addresses so equivalent textual forms (e.g. IPv6 with/without brackets) match.
            return InetAddress.getByName(host).equals(nodeAddress);
        }
        catch (UnknownHostException e)
        {
            LOG.debug("Unable to parse coordinator address '{}' for repair session {}",
                    session.get(SESSION_KEY_COORDINATOR), session.get(SESSION_KEY_ID));
            return false;
        }
    }

    /**
     * Extracts the host portion from Cassandra's {@code InetAddressAndPort.toString()}, which the JMX map exposes as
     * the {@code COORDINATOR} value. Handles the leading slash and the optional {@code :port} suffix for both IPv4
     * ({@code /127.0.0.1:7000}) and bracketed IPv6 ({@code /[::1]:7000}) forms, and strips any IPv6 scope id.
     *
     * @param coordinator the raw COORDINATOR value.
     * @return the bare host string, or {@code null} if it cannot be determined.
     */
    private static String extractHost(final String coordinator)
    {
        if (coordinator == null)
        {
            return null;
        }
        String value = coordinator.trim();
        if (value.startsWith("/"))
        {
            value = value.substring(1);
        }
        if (value.isEmpty())
        {
            return null;
        }
        String host;
        if (value.startsWith("["))
        {
            // Bracketed IPv6: [addr]:port -> take what is inside the brackets.
            int end = value.indexOf(']');
            if (end < 0)
            {
                return null;
            }
            host = value.substring(1, end);
        }
        else if (value.chars().filter(c -> c == ':').count() == SINGLE_COLON)
        {
            // Exactly one colon: IPv4 host:port.
            host = value.substring(0, value.indexOf(':'));
        }
        else
        {
            // No colon (bare IPv4) or many colons (bare/unbracketed IPv6): use as-is.
            host = value;
        }
        int scope = host.indexOf('%');
        if (scope >= 0)
        {
            host = host.substring(0, scope);
        }
        return host.isEmpty() ? null : host;
    }

    private boolean isHung(final Map<String, String> session)
    {
        if (!STATE_REPAIRING.equalsIgnoreCase(session.get(SESSION_KEY_STATE)))
        {
            return false;
        }
        String lastUpdate = session.get(SESSION_KEY_LAST_UPDATE);
        if (lastUpdate == null)
        {
            return false;
        }
        try
        {
            // LAST_UPDATE is an ABSOLUTE epoch timestamp in SECONDS (Cassandra's session.getLastUpdate() uses
            // nowInSeconds()). It is neither elapsed time nor milliseconds, so staleness is now - LAST_UPDATE.
            long lastUpdateMs = TimeUnit.SECONDS.toMillis(Long.parseLong(lastUpdate.trim()));
            long ageMs = myClock.getAsLong() - lastUpdateMs;
            return ageMs >= myStallThresholdMs;
        }
        catch (NumberFormatException e)
        {
            LOG.debug("Unable to parse LAST_UPDATE '{}' for repair session {}", lastUpdate,
                    session.get(SESSION_KEY_ID));
            return false;
        }
    }

    @Override
    public void close()
    {
        myScheduler.shutdown();
        try
        {
            if (!myScheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
            {
                myScheduler.shutdownNow();
            }
        }
        catch (InterruptedException e)
        {
            myScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
