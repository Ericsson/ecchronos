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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TestRepairSessionInfo
{
    private static Map<String, String> sessionWithCoordinator(final String coordinator)
    {
        Map<String, String> session = new HashMap<>();
        session.put(RepairSessionInfo.SESSION_ID, "session-1");
        session.put(RepairSessionInfo.COORDINATOR, coordinator);
        return session;
    }

    @Test
    public void testExtractHostIpv4WithSlashAndPort()
    {
        assertThat(RepairSessionInfo.extractHost("/127.0.0.1:7000")).isEqualTo("127.0.0.1");
    }

    @Test
    public void testExtractHostIpv4WithoutPort()
    {
        assertThat(RepairSessionInfo.extractHost("/127.0.0.1")).isEqualTo("127.0.0.1");
    }

    @Test
    public void testExtractHostBracketedIpv6WithPort()
    {
        assertThat(RepairSessionInfo.extractHost("/[0:0:0:0:0:0:0:1]:7000")).isEqualTo("0:0:0:0:0:0:0:1");
    }

    @Test
    public void testExtractHostBareIpv6()
    {
        assertThat(RepairSessionInfo.extractHost("/0:0:0:0:0:0:0:1")).isEqualTo("0:0:0:0:0:0:0:1");
    }

    @Test
    public void testExtractHostStripsScopeId()
    {
        assertThat(RepairSessionInfo.extractHost("/fe80:0:0:0:0:0:0:1%eth0")).isEqualTo("fe80:0:0:0:0:0:0:1");
    }

    @Test
    public void testExtractHostNullOrEmpty()
    {
        assertThat(RepairSessionInfo.extractHost(null)).isNull();
        assertThat(RepairSessionInfo.extractHost("/")).isNull();
    }

    @Test
    public void testIsCoordinatedByMatchesIpv4() throws Exception
    {
        InetAddress node = InetAddress.getByName("127.0.0.1");
        assertThat(RepairSessionInfo.isCoordinatedBy(sessionWithCoordinator("/127.0.0.1:7000"), node)).isTrue();
    }

    @Test
    public void testIsCoordinatedByMatchesBracketedIpv6() throws Exception
    {
        InetAddress node = InetAddress.getByName("::1");
        assertThat(RepairSessionInfo.isCoordinatedBy(sessionWithCoordinator("/[0:0:0:0:0:0:0:1]:7000"), node))
                .isTrue();
    }

    @Test
    public void testIsCoordinatedByDoesNotMatchDifferentAddress() throws Exception
    {
        InetAddress node = InetAddress.getByName("127.0.0.1");
        assertThat(RepairSessionInfo.isCoordinatedBy(sessionWithCoordinator("/10.0.0.99:7000"), node)).isFalse();
    }

    @Test
    public void testIsCoordinatedByHandlesMissingCoordinator() throws Exception
    {
        InetAddress node = InetAddress.getByName("127.0.0.1");
        assertThat(RepairSessionInfo.isCoordinatedBy(new HashMap<>(), node)).isFalse();
    }
}
