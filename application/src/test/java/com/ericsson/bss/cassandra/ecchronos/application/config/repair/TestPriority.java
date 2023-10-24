/*
 * Copyright 2023 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.repair;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestPriority
{
    private Priority priority;

    @Before
    public void setUp() {
        priority = new Priority();
    }

    @Test
    public void testDefaultConstructorSetsHours()
    {
        assertThat(priority.getPriorityGranularityUnit()).isEqualTo(TimeUnit.HOURS);
    }

    @Test
    public void testSetValidGranularityUnit()
    {
        priority.setPriorityGranularityUnit(TimeUnit.MINUTES);
        assertThat(priority.getPriorityGranularityUnit()).isEqualTo(TimeUnit.MINUTES);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetInvalidGranularityUnit()
    {
        priority.setPriorityGranularityUnit(TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNullGranularityUnit()
    {
        priority.setPriorityGranularityUnit(null);
    }
}
