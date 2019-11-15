/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.osgi.commands;

import java.util.TimeZone;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPrintUtils
{
    @Test
    public void testEpochToHumanReadable()
    {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        assertThat(PrintUtils.epochToHumanReadable(0)).isEqualTo("1970-01-01 00:00:00");
        assertThat(PrintUtils.epochToHumanReadable(1573720152000L)).isEqualTo("2019-11-14 08:29:12");
    }

    @Test
    public void testToPercentage()
    {
        assertThat(PrintUtils.toPercentage(0.1)).isEqualTo("10%");
        assertThat(PrintUtils.toPercentage(0.344)).isEqualTo("34%");
        assertThat(PrintUtils.toPercentage(0.345)).isEqualTo("35%");
    }
}
