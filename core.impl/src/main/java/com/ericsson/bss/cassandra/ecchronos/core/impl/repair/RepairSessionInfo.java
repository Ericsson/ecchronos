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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for interpreting the repair session maps returned by
 * {@link com.ericsson.bss.cassandra.ecchronos.core.jmx.DistributedJmxProxy#getRepairSessions(java.util.UUID)}.
 * <p>
 * The map keys and value semantics come from Cassandra's
 * {@code org.apache.cassandra.repair.consistent.LocalSessionInfo}:
 * <ul>
 *   <li>{@code SESSION_ID} — {@code session.sessionID.toString()}</li>
 *   <li>{@code STATE} — {@code ConsistentSession.State} enum name, e.g. {@code REPAIRING}</li>
 *   <li>{@code STARTED} — absolute epoch seconds when the session started</li>
 *   <li>{@code LAST_UPDATE} — absolute epoch seconds of the last session update
 *       ({@code session.getLastUpdate()}, {@code nowInSeconds()})</li>
 *   <li>{@code COORDINATOR} — {@code InetAddressAndPort.toString()} of the coordinator,
 *       e.g. {@code /127.0.0.1:7000} or {@code /[::1]:7000}</li>
 *   <li>{@code PARTICIPANTS} — comma-separated participant host addresses</li>
 *   <li>{@code PARTICIPANTS_WP} — comma-separated participant host addresses with port</li>
 *   <li>{@code TABLES} — comma-separated {@code keyspace.table} names</li>
 * </ul>
 */
public final class RepairSessionInfo
{
    private static final Logger LOG = LoggerFactory.getLogger(RepairSessionInfo.class);

    /** Key for the repair session id. */
    public static final String SESSION_ID = "SESSION_ID";
    /** Key for the repair session state (see {@code ConsistentSession.State}). */
    public static final String STATE = "STATE";
    /** Key for the epoch-seconds timestamp when the session started. */
    public static final String STARTED = "STARTED";
    /** Key for the epoch-seconds timestamp of the last session update. */
    public static final String LAST_UPDATE = "LAST_UPDATE";
    /** Key for the coordinator address ({@code InetAddressAndPort.toString()}). */
    public static final String COORDINATOR = "COORDINATOR";
    /** Key for the comma-separated participant host addresses. */
    public static final String PARTICIPANTS = "PARTICIPANTS";
    /** Key for the comma-separated participant host addresses including port. */
    public static final String PARTICIPANTS_WP = "PARTICIPANTS_WP";
    /** Key for the comma-separated {@code keyspace.table} names. */
    public static final String TABLES = "TABLES";

    private static final long SINGLE_COLON = 1L;

    private RepairSessionInfo()
    {
        // Utility class.
    }

    /**
     * Whether the given session is coordinated by the node with the given address.
     *
     * @param session the session map from {@code getRepairSessions}.
     * @param nodeAddress the broadcast address of the node to test.
     * @return {@code true} if the session's {@code COORDINATOR} resolves to {@code nodeAddress}.
     */
    public static boolean isCoordinatedBy(final Map<String, String> session, final InetAddress nodeAddress)
    {
        String host = extractHost(session.get(COORDINATOR));
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
                    session.get(COORDINATOR), session.get(SESSION_ID));
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
    public static String extractHost(final String coordinator)
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
}
