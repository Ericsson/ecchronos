/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import nl.jqno.equalsverifier.Warning;
import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTLSConfig
{
    @Test
    public void testSetCipherSuites()
    {
        TLSConfig tlsConfig = new TLSConfig();

        tlsConfig.setCipherSuites("test");
        assertThat(tlsConfig.getCipherSuites()).isPresent();
        assertThat(tlsConfig.getCipherSuites().get()).containsExactly("test");

        tlsConfig.setCipherSuites("test,test2");
        assertThat(tlsConfig.getCipherSuites()).isPresent();
        assertThat(tlsConfig.getCipherSuites().get()).containsExactly("test", "test2");

        tlsConfig.setCipherSuites(null);
        assertThat(tlsConfig.getCipherSuites()).isEmpty();
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(TLSConfig.class).usingGetClass()
                .suppress(Warning.NONFINAL_FIELDS)
                .verify();
    }
}
