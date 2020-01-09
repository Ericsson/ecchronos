/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import static org.assertj.core.api.Assertions.assertThat;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class TestLongToken
{
    @Test
    public void testTokensEqual()
    {
        LongToken token1 = new LongToken(1);
        LongToken token2 = new LongToken(1);

        assertThat(token1).isEqualTo(token2);
        assertThat(token1.compareTo(token2)).isEqualTo(0);
        assertThat(token1.hashCode()).isEqualTo(token2.hashCode());
    }

    @Test
    public void testTokensNullNotEqual()
    {
        LongToken token1 = new LongToken(1);

        assertThat(token1).isNotEqualTo(null);
    }

    @Test
    public void testTokensNotEqual()
    {
        LongToken token1 = new LongToken(1);
        LongToken token2 = new LongToken(2);

        assertThat(token1).isNotEqualTo(token2);
        assertThat(token1.compareTo(token2)).isLessThan(0);
        assertThat(token2.compareTo(token1)).isGreaterThan(0);
        assertThat(token1.hashCode()).isNotEqualTo(token2.hashCode());
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(LongToken.class).usingGetClass().verify();
    }
}
