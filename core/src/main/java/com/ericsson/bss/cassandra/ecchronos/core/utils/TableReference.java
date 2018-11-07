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

/**
 * A class containing keyspace/table mapping to avoid passing around two strings to refer to one specific table.
 */
public final class TableReference
{
    private final String myKeyspace;

    private final String myTable;

    public TableReference(String keyspace, String table)
    {
        myKeyspace = keyspace;
        myTable = table;
    }

    public String getTable()
    {
        return myTable;
    }

    public String getKeyspace()
    {
        return myKeyspace;
    }

    @Override
    public String toString()
    {
        return myKeyspace + "." + myTable;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableReference that = (TableReference) o;

        if (myKeyspace != null ? !myKeyspace.equals(that.myKeyspace) : that.myKeyspace != null) return false;
        return myTable != null ? myTable.equals(that.myTable) : that.myTable == null;

    }

    @Override
    public int hashCode()
    {
        int result = myKeyspace != null ? myKeyspace.hashCode() : 0;
        result = 31 * result + (myTable != null ? myTable.hashCode() : 0);
        return result;
    }
}
