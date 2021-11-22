/*
 * Copyright 2021 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.EndPointFactory;
import com.datastax.driver.core.Row;
import com.ericsson.bss.cassandra.ecchronos.connection.impl.EccEndPointFactory;

@RunWith(MockitoJUnitRunner.class)
public class TestEccEndPointFactory
{
    private static final InetSocketAddress defaultSocketAddress = InetSocketAddress.createUnresolved("localhost", 9042);

    private static final UUID localHostUuid = UUID.randomUUID();

    @Mock
    private EndPointFactory mockedEndPointFactory;

    @Mock
    private EndPoint localEndPoint;

    private EccEndPointFactory eccEndPointFactory;

    @Before
    public void init()
    {
        localEndPoint = mockedEndPoint();
        eccEndPointFactory = new EccEndPointFactory(localEndPoint, localHostUuid, mockedEndPointFactory);
    }

    @Test
    public void testCreateForLocalEndPoint()
    {
        Row endPointRow = mockedEndPointRow(localHostUuid);

        EndPoint endPoint = eccEndPointFactory.create(endPointRow);

        assertThat(endPoint, is(localEndPoint));
    }

    @Test
    public void testCreateWithNullHostId()
    {
        Row endPointRow = mockedEndPointRow(null);

        EndPoint endPoint = eccEndPointFactory.create(endPointRow);

        assertThat(endPoint, nullValue());
    }

    @Test
    public void testCreateWithNullDelegate()
    {
        Row endPointRow = mockedEndPointRow(UUID.randomUUID());
        when(mockedEndPointFactory.create(eq(endPointRow))).thenReturn(null);

        EndPoint endPoint = eccEndPointFactory.create(endPointRow);

        assertThat(endPoint, nullValue());
    }

    @Test
    public void testSimpleWrappedEndPoint()
    {
        UUID hostId = UUID.randomUUID();
        Row endPointRow = mockedEndPointRow(hostId);
        EndPoint firstEndPoint = mockedEndPoint();
        EndPoint secondEndPoint = mockedEndPoint();

        when(mockedEndPointFactory.create(eq(endPointRow))).thenReturn(firstEndPoint);
        EndPoint firstWrappedEndPoint = eccEndPointFactory.create(endPointRow);

        // Make sure to create a new source EndPoint object to compare equality on host UUID
        when(mockedEndPointFactory.create(eq(endPointRow))).thenReturn(secondEndPoint);
        EndPoint secondWrappedEndPoint = eccEndPointFactory.create(endPointRow);

        assertThat(firstEndPoint, not(firstWrappedEndPoint));
        assertThat(secondEndPoint, not(secondWrappedEndPoint));
        assertThat(firstWrappedEndPoint, is(secondWrappedEndPoint));
        assertThat(firstWrappedEndPoint.hashCode(), equalTo(secondWrappedEndPoint.hashCode()));

        // Make sure resolve call is delegated
        assertThat(firstWrappedEndPoint.resolve(), equalTo(defaultSocketAddress));
        verify(secondEndPoint).resolve();
    }

    private Row mockedEndPointRow(UUID uuid)
    {
        Row row = mock(Row.class);
        when(row.getUUID(eq("host_id"))).thenReturn(uuid);
        return row;
    }

    private EndPoint mockedEndPoint()
    {
        EndPoint endPoint = mock(EndPoint.class);
        when(endPoint.resolve()).thenReturn(defaultSocketAddress);
        return endPoint;
    }
}
