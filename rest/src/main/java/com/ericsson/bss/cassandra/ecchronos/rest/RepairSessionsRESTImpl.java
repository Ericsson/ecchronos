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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.impl.repair.RepairSessionInfo;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy;
import com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxyFactory;
import com.ericsson.bss.cassandra.ecchronos.core.metadata.DriverNode;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairSession;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * REST controller for listing and manually failing incremental repair sessions across managed nodes.
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
public class RepairSessionsRESTImpl implements RepairSessionsREST
{
    private static final long UNKNOWN_TIMESTAMP = -1L;

    private final DistributedJmxProxyFactory myJmxProxyFactory;
    private final DistributedNativeConnectionProvider myNativeConnectionProvider;

    /**
     * Constructs the repair sessions REST controller.
     *
     * @param jmxProxyFactory the JMX proxy factory.
     * @param nativeConnectionProvider the provider of managed nodes.
     */
    @Autowired
    public RepairSessionsRESTImpl(final DistributedJmxProxyFactory jmxProxyFactory,
            final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        myJmxProxyFactory = jmxProxyFactory;
        myNativeConnectionProvider = nativeConnectionProvider;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairSessions",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repair-sessions",
            description = "List incremental repair sessions across managed nodes.",
            summary = "List incremental repair sessions.")
    public final ResponseEntity<List<RepairSession>> getRepairSessions(
            @RequestParam(required = false)
            @Parameter(description = "Only list sessions reported by this managed node.")
            final String nodeID)
    {
        List<UUID> targetNodes = resolveTargetNodes(nodeID);
        List<RepairSession> result = new ArrayList<>();
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            for (UUID node : targetNodes)
            {
                for (Map<String, String> session : proxy.getRepairSessions(node))
                {
                    result.add(toRepairSession(node, session));
                }
            }
        }
        catch (IOException e)
        {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, "Unable to obtain a JMX connection", e);
        }
        return ResponseEntity.ok(result);
    }

    @Override
    @PostMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairSessions/{sessionId}/fail",
            produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "fail-repair-session",
            description = "Fail (cancel) an incremental repair session. With force=false the session is cancelled on "
                    + "its coordinator; with force=true it is force-failed on every managed node that reports it.",
            summary = "Fail an incremental repair session.")
    public final ResponseEntity<List<RepairSession>> failRepairSession(
            @PathVariable
            @Parameter(description = "The id of the session to fail.")
            final String sessionId,
            @RequestParam(required = false, defaultValue = "false")
            @Parameter(description = "Force-fail on all participant nodes instead of only the coordinator.")
            final boolean force,
            @RequestParam(required = false)
            @Parameter(description = "Restrict the operation to a single managed node.")
            final String nodeID)
    {
        List<UUID> targetNodes = resolveTargetNodes(nodeID);
        List<RepairSession> failed = new ArrayList<>();
        try (DistributedJmxProxy proxy = myJmxProxyFactory.connect())
        {
            if (force)
            {
                failed.addAll(failOnAllReportingNodes(proxy, targetNodes, sessionId));
            }
            else
            {
                failed.add(failOnCoordinator(proxy, targetNodes, sessionId));
            }
        }
        catch (IOException e)
        {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, "Unable to obtain a JMX connection", e);
        }
        return ResponseEntity.ok(failed);
    }

    private List<RepairSession> failOnAllReportingNodes(final DistributedJmxProxy proxy, final List<UUID> targetNodes,
            final String sessionId)
    {
        List<RepairSession> failed = new ArrayList<>();
        for (UUID node : targetNodes)
        {
            for (Map<String, String> session : proxy.getRepairSessions(node))
            {
                if (sessionId.equals(session.get(RepairSessionInfo.SESSION_ID)))
                {
                    proxy.failRepairSession(node, sessionId, true);
                    failed.add(toRepairSession(node, session));
                }
            }
        }
        if (failed.isEmpty())
        {
            throw new ResponseStatusException(NOT_FOUND,
                    "No managed node reports repair session " + sessionId);
        }
        return failed;
    }

    private RepairSession failOnCoordinator(final DistributedJmxProxy proxy, final List<UUID> targetNodes,
            final String sessionId)
    {
        for (UUID node : targetNodes)
        {
            InetAddress nodeAddress = broadcastAddress(node);
            if (nodeAddress == null)
            {
                continue;
            }
            for (Map<String, String> session : proxy.getRepairSessions(node))
            {
                if (sessionId.equals(session.get(RepairSessionInfo.SESSION_ID))
                        && RepairSessionInfo.isCoordinatedBy(session, nodeAddress))
                {
                    proxy.failRepairSession(node, sessionId, false);
                    return toRepairSession(node, session);
                }
            }
        }
        throw new ResponseStatusException(NOT_FOUND, "No managed node is the coordinator of repair session "
                + sessionId + "; retry with force=true to fail it on participant nodes");
    }

    private List<UUID> resolveTargetNodes(final String nodeID)
    {
        Map<UUID, Node> nodes = myNativeConnectionProvider.getNodes();
        if (nodeID == null)
        {
            return new ArrayList<>(nodes.keySet());
        }
        UUID parsed = parseNodeId(nodeID);
        if (!nodes.containsKey(parsed))
        {
            throw new ResponseStatusException(NOT_FOUND, "Node " + nodeID + " is not managed by ecChronos");
        }
        return List.of(parsed);
    }

    private static UUID parseNodeId(final String nodeID)
    {
        try
        {
            return UUID.fromString(nodeID);
        }
        catch (IllegalArgumentException e)
        {
            throw new ResponseStatusException(BAD_REQUEST, "Invalid nodeID: " + nodeID, e);
        }
    }

    private InetAddress broadcastAddress(final UUID nodeID)
    {
        Node node = myNativeConnectionProvider.getNodes().get(nodeID);
        if (node == null)
        {
            return null;
        }
        return new DriverNode(node).getPublicAddress();
    }

    private static RepairSession toRepairSession(final UUID nodeID, final Map<String, String> session)
    {
        return new RepairSession(
                nodeID,
                session.get(RepairSessionInfo.SESSION_ID),
                session.get(RepairSessionInfo.STATE),
                session.get(RepairSessionInfo.COORDINATOR),
                parseEpochSeconds(session.get(RepairSessionInfo.STARTED)),
                parseEpochSeconds(session.get(RepairSessionInfo.LAST_UPDATE)),
                session.get(RepairSessionInfo.PARTICIPANTS),
                session.get(RepairSessionInfo.TABLES));
    }

    private static long parseEpochSeconds(final String value)
    {
        if (value == null)
        {
            return UNKNOWN_TIMESTAMP;
        }
        try
        {
            return Long.parseLong(value.trim());
        }
        catch (NumberFormatException e)
        {
            return UNKNOWN_TIMESTAMP;
        }
    }
}
