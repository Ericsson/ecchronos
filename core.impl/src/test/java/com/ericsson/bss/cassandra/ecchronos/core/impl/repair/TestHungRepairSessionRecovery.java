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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestHungRepairSessionRecovery
{
    private static final long STALL_THRESHOLD_MS = TimeUnit.MINUTES.toMillis(30);
    private static final long SCAN_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final String COORDINATOR_IP = "127.0.0.1";
    // Fixed "now" so tests are deterministic: 2026-01-01T00:00:00Z (epoch seconds and the equivalent millis).
    private static final long NOW_SECONDS = 1_767_225_600L;
    private static final long NOW_MS = TimeUnit.SECONDS.toMillis(NOW_SECONDS);
    private static final LongSupplier FIXED_CLOCK = () -> NOW_MS;

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private DistributedJmxProxy myJmxProxy;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node myNode;
    @Mock
    private Node mySecondNode;

    private final UUID myNodeId = UUID.randomUUID();
    private final UUID mySecondNodeId = UUID.randomUUID();
    private HungRepairSessionRecovery myRecovery;

    @Before
    public void setup() throws Exception
    {
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
        when(myNativeConnectionProvider.getNodes()).thenReturn(Map.of(myNodeId, myNode));
        InetSocketAddress broadcast = new InetSocketAddress(InetAddress.getByName(COORDINATOR_IP), 7000);
        when(myNode.getBroadcastAddress()).thenReturn(Optional.of(broadcast));
    }

    @After
    public void cleanup()
    {
        if (myRecovery != null)
        {
            myRecovery.close();
        }
    }

    private HungRepairSessionRecovery newRecovery(final boolean enabled, final long thresholdMs)
    {
        return newRecovery(enabled, thresholdMs, false, false);
    }

    private HungRepairSessionRecovery newRecovery(final boolean enabled, final long thresholdMs,
            final boolean bypassCoordinatorCheck)
    {
        return newRecovery(enabled, thresholdMs, bypassCoordinatorCheck, false);
    }

    private HungRepairSessionRecovery newRecovery(final boolean enabled, final long thresholdMs,
            final boolean bypassCoordinatorCheck, final boolean force)
    {
        return new HungRepairSessionRecovery(myJmxProxyFactory, myNativeConnectionProvider, enabled, thresholdMs,
                SCAN_INTERVAL_MS, bypassCoordinatorCheck, force, FIXED_CLOCK);
    }

    /**
     * Builds a session map with the real Cassandra LocalSessionInfo keys. lastUpdateSeconds is the ABSOLUTE epoch
     * seconds of the last update (not elapsed time).
     */
    private static Map<String, String> session(final String id, final String state, final String coordinator,
            final long lastUpdateSeconds)
    {
        Map<String, String> session = new HashMap<>();
        session.put("SESSION_ID", id);
        session.put("STATE", state);
        session.put("COORDINATOR", coordinator);
        session.put("LAST_UPDATE", Long.toString(lastUpdateSeconds));
        return session;
    }

    private void twoNodeTopology() throws Exception
    {
        InetSocketAddress secondBroadcast = new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 7000);
        when(mySecondNode.getBroadcastAddress()).thenReturn(Optional.of(secondBroadcast));
        Map<UUID, Node> nodes = new LinkedHashMap<>();
        nodes.put(myNodeId, myNode);
        nodes.put(mySecondNodeId, mySecondNode);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodes);
    }

    @Test
    public void testDisabledDoesNothing()
    {
        myRecovery = newRecovery(false, STALL_THRESHOLD_MS);

        myRecovery.scan();

        verify(myJmxProxy, never()).getRepairSessions(eq(myNodeId));
        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testCancelsHungCoordinatedSession()
    {
        // Last update 45 minutes ago -> older than the 30 minute threshold.
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        List<Map<String, String>> sessions = Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate));
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(sessions);

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
    }

    @Test
    public void testDoesNotCancelSessionBelowThreshold()
    {
        // Last update 10 minutes ago -> younger than the 30 minute threshold.
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(10);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP, lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testDoesNotCancelFreshlyUpdatedSession()
    {
        // A session updated "now" must never be treated as hung, guarding against the epoch-vs-elapsed bug.
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP, NOW_SECONDS)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testDoesNotCancelNonRepairingSession()
    {
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "FINALIZED", "/" + COORDINATOR_IP, lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testDoesNotCancelSessionCoordinatedByAnotherNode()
    {
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/10.0.0.99:7000", lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testMatchesCoordinatorWithoutPort()
    {
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP, lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
    }

    @Test
    public void testMatchesBracketedIpv6Coordinator() throws Exception
    {
        // Node broadcasts an IPv6 loopback; coordinator reported as bracketed IPv6 with port.
        InetSocketAddress broadcast = new InetSocketAddress(InetAddress.getByName("::1"), 7000);
        when(myNode.getBroadcastAddress()).thenReturn(Optional.of(broadcast));

        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/[0:0:0:0:0:0:0:1]:7000", lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
    }

    @Test
    public void testIgnoresSessionWithUnparseableLastUpdate()
    {
        Map<String, String> bad = session("session-2", "REPAIRING", "/" + COORDINATOR_IP, 0L);
        bad.put("LAST_UPDATE", "not-a-number");
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(bad));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy, never()).failRepairSession(eq(myNodeId), eq("session-2"), anyBoolean());
    }

    @Test
    public void testBypassFailsSessionReportedByNonCoordinatorNode() throws Exception
    {
        twoNodeTopology();
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        // The session is coordinated by the first node but is also reported by the second (non-coordinator) node.
        // With bypass off the second node would skip it; with bypass on the second node fails it too.
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate)));
        when(myJmxProxy.getRepairSessions(mySecondNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS, true);
        myRecovery.scan();

        // Failed on the coordinator (first node) and on the non-coordinator node that also reported it.
        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
        verify(myJmxProxy).failRepairSession(mySecondNodeId, "session-1", false);
    }

    @Test
    public void testBypassFailsSessionCoordinatedByAnotherNodeOnlyWhereReported() throws Exception
    {
        twoNodeTopology();
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        // Coordinator is a node we do not manage, and only the first node reports the session. With bypass on it is
        // cancelled on the node that reported it, but not on a node that never listed it.
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/10.0.0.99:7000", lastUpdate)));
        when(myJmxProxy.getRepairSessions(mySecondNodeId)).thenReturn(Collections.emptyList());

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS, true);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
        verify(myJmxProxy, never()).failRepairSession(eq(mySecondNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testBypassSwallowsFaultAndContinuesOnOtherNodes() throws Exception
    {
        twoNodeTopology();
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        // Both nodes report the same session; the first node's fail throws an expected fault. It must be swallowed
        // so the scan continues and the second node is still attempted.
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/10.0.0.99:7000", lastUpdate)));
        when(myJmxProxy.getRepairSessions(mySecondNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/10.0.0.99:7000", lastUpdate)));
        doThrow(new RuntimeException("jmx down")).when(myJmxProxy).failRepairSession(myNodeId, "session-1", false);

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS, true);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
        verify(myJmxProxy).failRepairSession(mySecondNodeId, "session-1", false);
    }

    @Test
    public void testBypassDoesNotActOnNodeThatDoesNotReportSession() throws Exception
    {
        twoNodeTopology();
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        // Only the first node reports the hung session; the second reports nothing. Even with bypass on, the second
        // node must not be touched because it never listed the session.
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate)));
        when(myJmxProxy.getRepairSessions(mySecondNodeId)).thenReturn(Collections.emptyList());

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS, true);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
        verify(myJmxProxy, never()).failRepairSession(eq(mySecondNodeId), eq("session-1"), anyBoolean());
    }

    @Test
    public void testSetBypassCoordinatorCheck()
    {
        myRecovery = newRecovery(false, STALL_THRESHOLD_MS);
        assertThat(myRecovery.isBypassCoordinatorCheck()).isFalse();

        myRecovery.setBypassCoordinatorCheck(true);

        assertThat(myRecovery.isBypassCoordinatorCheck()).isTrue();
    }

    @Test
    public void testForceIsPassedToFailSession()
    {
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate)));

        // Coordinator path, but force is enabled -> failSession must be invoked with force=true.
        myRecovery = newRecovery(true, STALL_THRESHOLD_MS, false, true);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", true);
    }

    @Test
    public void testForceDefaultsToFalse()
    {
        long lastUpdate = NOW_SECONDS - TimeUnit.MINUTES.toSeconds(45);
        when(myJmxProxy.getRepairSessions(myNodeId)).thenReturn(Arrays.asList(
                session("session-1", "REPAIRING", "/" + COORDINATOR_IP + ":7000", lastUpdate)));

        myRecovery = newRecovery(true, STALL_THRESHOLD_MS);
        myRecovery.scan();

        verify(myJmxProxy).failRepairSession(myNodeId, "session-1", false);
    }

    @Test
    public void testSetForce()
    {
        myRecovery = newRecovery(false, STALL_THRESHOLD_MS);
        assertThat(myRecovery.isForce()).isFalse();

        myRecovery.setForce(true);

        assertThat(myRecovery.isForce()).isTrue();
    }

    @Test
    public void testSetEnabledAndThreshold()
    {
        myRecovery = newRecovery(false, STALL_THRESHOLD_MS);
        assertThat(myRecovery.isEnabled()).isFalse();

        myRecovery.setEnabled(true);
        myRecovery.setStallThresholdMs(1234L);

        assertThat(myRecovery.isEnabled()).isTrue();
        assertThat(myRecovery.getStallThresholdMs()).isEqualTo(1234L);
    }

    @Test
    public void testSetInvalidThresholdRejected()
    {
        myRecovery = newRecovery(false, STALL_THRESHOLD_MS);
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> myRecovery.setStallThresholdMs(0));
    }

    @Test
    public void testConstructorRejectsInvalidThreshold()
    {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> newRecovery(false, 0));
    }
}
