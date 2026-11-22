/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.utils.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TestUnitConverter
{
    @Test
    public void testLargestRepresentableSizes()
    {
        assertThat(UnitConverter.toBytes("9007199254740991k")).isEqualTo(Long.MAX_VALUE - 1023);
        assertThat(UnitConverter.toBytes("8796093022207M")).isEqualTo(Long.MAX_VALUE - 1048575);
        assertThat(UnitConverter.toBytes("8589934591g")).isEqualTo(Long.MAX_VALUE - 1073741823);
        assertThat(UnitConverter.toBytes(Long.toString(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testRejectsOverflowDuringUnitConversion()
    {
        for (String size : new String[]{"9007199254740992K", "8796093022208m", "8589934592G", "17179869184G"})
        {
            assertThatThrownBy(() -> UnitConverter.toBytes(size))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(size);
        }
    }
}
