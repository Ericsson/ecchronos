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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairSession;

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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRepairSessionsRESTImpl
{
    private static final String NODE1_IP = "127.0.0.1";
    private static final String NODE2_IP = "127.0.0.2";

    @Mock
    private DistributedJmxProxyFactory myJmxProxyFactory;
    @Mock
    private DistributedJmxProxy myJmxProxy;
    @Mock
    private DistributedNativeConnectionProvider myNativeConnectionProvider;
    @Mock
    private Node myNode1;
    @Mock
    private Node myNode2;

    private final UUID myNode1Id = UUID.randomUUID();
    private final UUID myNode2Id = UUID.randomUUID();

    private RepairSessionsRESTImpl myController;

    @Before
    public void setup() throws Exception
    {
        when(myJmxProxyFactory.connect()).thenReturn(myJmxProxy);
        Map<UUID, Node> nodes = new LinkedHashMap<>();
        nodes.put(myNode1Id, myNode1);
        nodes.put(myNode2Id, myNode2);
        when(myNativeConnectionProvider.getNodes()).thenReturn(nodes);
        when(myNode1.getBroadcastAddress())
                .thenReturn(Optional.of(new InetSocketAddress(InetAddress.getByName(NODE1_IP), 7000)));
        when(myNode2.getBroadcastAddress())
                .thenReturn(Optional.of(new InetSocketAddress(InetAddress.getByName(NODE2_IP), 7000)));
        myController = new RepairSessionsRESTImpl(myJmxProxyFactory, myNativeConnectionProvider);
    }

    private static Map<String, String> session(final String id, final String state, final String coordinator)
    {
        Map<String, String> session = new HashMap<>();
        session.put("SESSION_ID", id);
        session.put("STATE", state);
        session.put("COORDINATOR", coordinator);
        session.put("STARTED", "1767225600");
        session.put("LAST_UPDATE", "1767225700");
        session.put("PARTICIPANTS", "127.0.0.1,127.0.0.2");
        session.put("TABLES", "ks.tbl");
        return session;
    }

    @Test
    public void testListAllNodes()
    {
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));
        when(myJmxProxy.getRepairSessions(myNode2Id)).thenReturn(Arrays.asList(
                session("s2", "FINALIZED", "/" + NODE2_IP + ":7000")));

        ResponseEntity<List<RepairSession>> response = myController.getRepairSessions(null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        List<RepairSession> sessions = response.getBody();
        assertThat(sessions).hasSize(2);
        assertThat(sessions).extracting(s -> s.sessionId).containsExactlyInAnyOrder("s1", "s2");
        RepairSession s1 = sessions.stream().filter(s -> s.sessionId.equals("s1")).findFirst().orElseThrow();
        assertThat(s1.nodeID).isEqualTo(myNode1Id);
        assertThat(s1.state).isEqualTo("REPAIRING");
        assertThat(s1.started).isEqualTo(1767225600L);
        assertThat(s1.lastUpdate).isEqualTo(1767225700L);
        assertThat(s1.tables).isEqualTo("ks.tbl");
    }

    @Test
    public void testListSingleNode()
    {
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));

        ResponseEntity<List<RepairSession>> response = myController.getRepairSessions(myNode1Id.toString());

        assertThat(response.getBody()).hasSize(1);
        verify(myJmxProxy, never()).getRepairSessions(myNode2Id);
    }

    @Test
    public void testListUnknownNodeReturns404()
    {
        assertThatExceptionOfType(ResponseStatusException.class)
                .isThrownBy(() -> myController.getRepairSessions(UUID.randomUUID().toString()))
                .matches(e -> e.getStatusCode() == HttpStatus.NOT_FOUND);
    }

    @Test
    public void testListInvalidNodeReturns400()
    {
        assertThatExceptionOfType(ResponseStatusException.class)
                .isThrownBy(() -> myController.getRepairSessions("not-a-uuid"))
                .matches(e -> e.getStatusCode() == HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testFailOnCoordinatorUsesForceFalse()
    {
        // s1 is coordinated by node1; node2 does not report it.
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));
        when(myJmxProxy.getRepairSessions(myNode2Id)).thenReturn(Collections.emptyList());

        ResponseEntity<List<RepairSession>> response = myController.failRepairSession("s1", false, null);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).hasSize(1);
        assertThat(response.getBody().get(0).nodeID).isEqualTo(myNode1Id);
        verify(myJmxProxy).failRepairSession(myNode1Id, "s1", false);
        verify(myJmxProxy, never()).failRepairSession(myNode2Id, "s1", false);
    }

    @Test
    public void testFailNoCoordinatorFoundReturns404()
    {
        // s1 is reported by node1 but coordinated by an unmanaged node.
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/10.0.0.99:7000")));
        when(myJmxProxy.getRepairSessions(myNode2Id)).thenReturn(Collections.emptyList());

        assertThatExceptionOfType(ResponseStatusException.class)
                .isThrownBy(() -> myController.failRepairSession("s1", false, null))
                .matches(e -> e.getStatusCode() == HttpStatus.NOT_FOUND);
        verify(myJmxProxy, never()).failRepairSession(myNode1Id, "s1", false);
    }

    @Test
    public void testForceFailsOnAllReportingNodes()
    {
        // Both nodes report s1 (participant on node2); force fails it on both.
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));
        when(myJmxProxy.getRepairSessions(myNode2Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));

        ResponseEntity<List<RepairSession>> response = myController.failRepairSession("s1", true, null);

        assertThat(response.getBody()).hasSize(2);
        verify(myJmxProxy).failRepairSession(myNode1Id, "s1", true);
        verify(myJmxProxy).failRepairSession(myNode2Id, "s1", true);
    }

    @Test
    public void testForceFailUnknownSessionReturns404()
    {
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Collections.emptyList());
        when(myJmxProxy.getRepairSessions(myNode2Id)).thenReturn(Collections.emptyList());

        assertThatExceptionOfType(ResponseStatusException.class)
                .isThrownBy(() -> myController.failRepairSession("missing", true, null))
                .matches(e -> e.getStatusCode() == HttpStatus.NOT_FOUND);
    }

    @Test
    public void testFailForceScopedToSingleNode()
    {
        when(myJmxProxy.getRepairSessions(myNode1Id)).thenReturn(Arrays.asList(
                session("s1", "REPAIRING", "/" + NODE1_IP + ":7000")));

        ResponseEntity<List<RepairSession>> response = myController.failRepairSession("s1", true, myNode1Id.toString());

        assertThat(response.getBody()).hasSize(1);
        verify(myJmxProxy).failRepairSession(myNode1Id, "s1", true);
        verify(myJmxProxy, never()).getRepairSessions(myNode2Id);
    }
}
