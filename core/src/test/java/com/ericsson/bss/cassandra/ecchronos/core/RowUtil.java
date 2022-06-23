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
package com.ericsson.bss.cassandra.ecchronos.core;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.google.common.collect.Sets;

public class RowUtil
{

    public static Row getRepairHistoryRow(String status, long range_begin, long range_end, long started_at) throws UnknownHostException
    {
        Row row = mock(Row.class);

        when(row.getSet(eq("participants"), eq(InetAddress.class))).thenReturn(Sets.newHashSet(InetAddress.getLocalHost()));
        when(row.getString(eq("status"))).thenReturn(status);
        when(row.getString(eq("range_begin"))).thenReturn(Long.toString(range_begin));
        when(row.getString(eq("range_end"))).thenReturn(Long.toString(range_end));
        when(row.getUuid(eq("id"))).thenReturn(Uuids.startOf(started_at));

        return row;
    }
}
