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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for reverse DNS resolution operations.
 * Provides methods to resolve IP addresses and hostnames to their canonical forms.
 */
public final class ReverseDNS
{
    private static final Logger LOG = LoggerFactory.getLogger(ReverseDNS.class);
    private static final Pattern IPV4_PREFIX_DEFAULT = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.){4}");
    private static final Pattern IPV6_PREFIX_DEFAULT = Pattern.compile("^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}\\.");

    private static final Pattern IPV4_PREFIX_K8S = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\-){3}((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d))\\.");
    private static final Pattern IPV6_PREFIX_K8S = Pattern.compile("^([0-9a-fA-F]{1,4}\\-){2,}([0-9a-fA-F]{0,4}\\-)*[0-9a-fA-F]{0,4}\\.");

    private ReverseDNS()
    {
        // Utility class
    }

    /**
     * Resolves a host string to its hostname using reverse DNS lookup.
     * Attempts canonical hostname first, then falls back to simple hostname.
     * Cleans IP prefixes from resolved hostnames when present.
     *
     * @param host the host string (IP address or hostname) to resolve
     * @return the resolved hostname, or original host if resolution fails
     * @throws UnknownHostException if the host cannot be resolved
     */
    public static String fromHostString(final String host) throws UnknownHostException
    {
        String resolvedHost = host;
        InetAddress addr = InetAddress.getByName(resolvedHost);
        String originalIP = addr.getHostAddress();

        // Try canonical hostname first (with DNS lookup)
        String canonicalHost = addr.getCanonicalHostName();
        if (!canonicalHost.equals(originalIP))
        {
            resolvedHost = cleanHostname(canonicalHost);
            LOG.debug("Resolved {} to {} using CanonicalHostName {}", host, resolvedHost, canonicalHost);
        }
        else
        {
            // Fallback to simple hostname (no DNS lookup)
            String simpleHost = addr.getHostName();
            resolvedHost = !simpleHost.equals(originalIP) ? cleanHostname(simpleHost) : host;
            LOG.debug("Resolved {} to {} using HostName {}", host, resolvedHost, simpleHost);
        }
        return resolvedHost;
    }

    /**
     * Removes IP prefix from hostname if present.
     * Handles: "10.244.1.5.pod-name.svc.cluster.local" -> "pod-name.svc.cluster.local"
     */
    private static String cleanHostname(final String hostname)
    {
        if (hostname == null || hostname.isEmpty())
        {
            return hostname;
        }

        // Remove IPv4 prefix
        if (IPV4_PREFIX_DEFAULT.matcher(hostname).find())
        {
            return IPV4_PREFIX_DEFAULT.matcher(hostname).replaceFirst("");
        }
        else if (IPV4_PREFIX_K8S.matcher(hostname).find())
        {
            return IPV4_PREFIX_K8S.matcher(hostname).replaceFirst("");
        }
        else if (IPV6_PREFIX_DEFAULT.matcher(hostname).find())
        {
            return IPV6_PREFIX_DEFAULT.matcher(hostname).replaceFirst("");
        }
        else if (IPV6_PREFIX_K8S.matcher(hostname).find())
        {
            return IPV6_PREFIX_K8S.matcher(hostname).replaceFirst("");
        }

        return hostname;
    }
}
