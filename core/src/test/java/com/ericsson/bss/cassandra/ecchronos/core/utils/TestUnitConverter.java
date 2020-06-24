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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestUnitConverter
{
    private static final long ONE_KiB = 1024L;
    private static final long ONE_MiB = 1024L * ONE_KiB;
    private static final long ONE_GiB = 1024L * ONE_MiB;

    @Test
    public void testByte()
    {
        assertThat(UnitConverter.toBytes("1234")).isEqualTo(1234L);
    }

    @Test
    public void testkByte()
    {
        assertThat(UnitConverter.toBytes("1k")).isEqualTo(ONE_KiB);
    }

    @Test
    public void testKByte()
    {
        assertThat(UnitConverter.toBytes("2K")).isEqualTo(2L * ONE_KiB);
    }

    @Test
    public void testmByte()
    {
        assertThat(UnitConverter.toBytes("1m")).isEqualTo(ONE_MiB);
    }

    @Test
    public void testMByte()
    {
        assertThat(UnitConverter.toBytes("2M")).isEqualTo(2L * ONE_MiB);
    }

    @Test
    public void testgByte()
    {
        assertThat(UnitConverter.toBytes("1g")).isEqualTo(ONE_GiB);
    }

    @Test
    public void testGByte()
    {
        assertThat(UnitConverter.toBytes("2G")).isEqualTo(2L * ONE_GiB);
    }

    @Test
    public void testInvalidCharacter()
    {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UnitConverter.toBytes(" 2G"));
    }

    @Test
    public void testInvalidUnit()
    {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UnitConverter.toBytes("2f"));
    }

    @Test
    public void testText()
    {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> UnitConverter.toBytes("m"));
    }
}
