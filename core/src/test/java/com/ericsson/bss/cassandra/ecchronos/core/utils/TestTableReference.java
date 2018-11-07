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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTableReference
{
    @Test
    public void testGetKeyspaceAndTableName()
    {
        String keyspace = "keyspace";
        String table = "table";

        TableReference tableReference = new TableReference(keyspace, table);

        assertThat(tableReference.getKeyspace()).isEqualTo(keyspace);
        assertThat(tableReference.getTable()).isEqualTo(table);
    }

    @Test
    public void testSameTableIsEqual()
    {
        TableReference tableReference1 = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table");

        assertThat(tableReference1).isEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isEqualTo(tableReference2.hashCode());
    }

    @Test
    public void testDifferentKeyspacesNotEqual()
    {
        TableReference tableReference1 = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace2", "table");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isNotEqualTo(tableReference2.hashCode());
    }

    @Test
    public void testDifferentTablesNotEqual()
    {
        TableReference tableReference1 = new TableReference("keyspace", "table");
        TableReference tableReference2 = new TableReference("keyspace", "table2");

        assertThat(tableReference1).isNotEqualTo(tableReference2);
        assertThat(tableReference1.hashCode()).isNotEqualTo(tableReference2.hashCode());
    }
}
