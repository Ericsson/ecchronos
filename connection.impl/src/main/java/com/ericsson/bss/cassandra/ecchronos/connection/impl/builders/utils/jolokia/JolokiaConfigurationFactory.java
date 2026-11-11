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
package com.ericsson.bss.cassandra.ecchronos.connection.impl.builders.utils.jolokia;

import org.jolokia.client.JolokiaClientBuilder.Configuration;

/**
 * Helper that produces a copy of a Jolokia {@link Configuration} with {@code Expect: 100-continue} disabled.
 *
 * <p>The Jolokia JMX adapter never calls {@code JolokiaClientBuilder.expectContinue(false)}, so the adapter's
 * {@link Configuration#expectContinue()} defaults to {@code true} and the JDK HTTP client sends an
 * {@code Expect: 100-continue} header on every request. That header is a root cause of the intermittent
 * {@code HTTP error 100} failures on the repair path (see issues #1549 / #1473). Since {@link Configuration} is an
 * immutable record we cannot mutate it in place; instead we reconstruct it through its canonical constructor with the
 * single {@code expectContinue} flag flipped to {@code false} and every other component preserved verbatim.
 *
 * <p>Calling the canonical constructor directly (rather than via reflection) means a Jolokia upgrade that changes the
 * record shape fails at compile time, which is the strongest possible version-drift guard.
 */
public final class JolokiaConfigurationFactory
{
    private JolokiaConfigurationFactory()
    {
        // Utility class
    }

    /**
     * Return a copy of the supplied configuration with {@code Expect: 100-continue} disabled.
     *
     * @param source the configuration handed to the custom {@code HttpClientBuilder} by the Jolokia client; must not
     *               be {@code null}.
     * @return a new {@link Configuration} identical to {@code source} except that {@link Configuration#expectContinue()}
     *         is {@code false}. If it is already {@code false} the {@code source} instance is returned unchanged.
     */
    public static Configuration withExpectContinueDisabled(final Configuration source)
    {
        if (!source.expectContinue())
        {
            return source;
        }
        return new Configuration(
                source.url(),
                source.user(),
                source.password(),
                source.proxy(),
                source.connectionConfig(),
                source.tlsConfig(),
                source.poolConfig(),
                source.contentCharset(),
                false,
                source.defaultHttpHeaders(),
                source.customizer(),
                source.clientBuilderClass());
    }
}
