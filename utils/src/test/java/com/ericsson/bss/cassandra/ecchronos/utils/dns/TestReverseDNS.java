/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.utils.dns;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.reflect.Method;

import org.junit.Test;

public class TestReverseDNS
{
    @Test
    public void testCleanHostnameWithIPPrefix()
    {
        // K8s scenario: IP concatenated with pod DNS
        String input = "10.244.1.5.cassandra-0.cassandra.default.svc.cluster.local";
        String expected = "cassandra-0.cassandra.default.svc.cluster.local";
        
        String result = invokeCleanHostname(input);
        assertEquals(expected, result);
    }

    @Test
    public void testCleanHostnameNormalFQDN()
    {
        // Normal FQDN without IP prefix
        String input = "cassandra-node1.example.com";
        String expected = "cassandra-node1.example.com";

        String result = invokeCleanHostname(input);
        assertEquals(expected, result);
    }

    @Test
    public void testCleanHostname_PlainIP()
    {
        // Plain IP address
        String input = "192.168.1.100";
        String expected = "192.168.1.100";

        String result = invokeCleanHostname(input);
        assertEquals(expected, result);
    }

    @Test
    public void testCleanHostnameEmptyAndNull()
    {
        assertNull(invokeCleanHostname(null));
        assertEquals("", invokeCleanHostname(""));
    }

    @Test
    public void testCleanHostnameIPv6()
    {
        // Real IPv6 scenario - test with actual IPv6 format
        String ipv6Input = "2001:db8::1.cassandra-0.cassandra.default.svc.cluster.local";
        String expected = "cassandra-0.cassandra.default.svc.cluster.local";

        String result = invokeCleanHostname(ipv6Input);
        assertEquals(expected, result);
    }

    @Test
    public void testCleanHostnameEdgeCases()
    {
        // Malformed IP prefix (incomplete IPv4)
        String malformed = "10.244.cassandra-0.default.svc.cluster.local";
        assertEquals(malformed, invokeCleanHostname(malformed)); // Should not match

        // IPv6 real format - should be cleaned
        String ipv6Raw = "2001:db8::1.pod-name.namespace.svc.cluster.local";
        assertEquals("pod-name.namespace.svc.cluster.local", invokeCleanHostname(ipv6Raw));

        // Edge case: only IP without DNS
        String onlyIPv4 = "192.168.1.100";
        assertEquals(onlyIPv4, invokeCleanHostname(onlyIPv4));
    }

    // Helper method to access private cleanHostname method via reflection
    private String invokeCleanHostname(String hostname)
    {
        try
        {
            // Extract IP from the test input for the new method signature
            String ip = null;
            if (hostname != null && hostname.startsWith("10.244.1.5."))
            {
                ip = "10.244.1.5";
            }
            else if (hostname != null && hostname.startsWith("2001:db8::1."))
            {
                ip = "2001:db8::1";
            }

            Method method = ReverseDNS.class.getDeclaredMethod("cleanHostname", String.class);
            method.setAccessible(true);
            return (String) method.invoke(null, hostname);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to invoke cleanHostname", e);
        }
    }
}