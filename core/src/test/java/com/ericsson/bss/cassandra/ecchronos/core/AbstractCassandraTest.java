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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;
import net.jcip.annotations.NotThreadSafe;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;

@NotThreadSafe
public abstract class AbstractCassandraTest
{
    protected static CassandraDaemonForECChronos myCassandraDaemon;
    protected static CqlSession mySession;

    private static NativeConnectionProvider myNativeConnectionProvider;

    @BeforeClass
    public static void setupCassandra() throws IOException
    {
        myCassandraDaemon = CassandraDaemonForECChronos.getInstance();

        mySession = myCassandraDaemon.getSession();

        Node tmpNode = null;

        InetAddress localhostAddress = InetAddress.getByName("localhost");

        for (Node node : mySession.getMetadata().getNodes().values())
        {
            if (node.getBroadcastAddress().get().getAddress().equals(localhostAddress))
            {
                tmpNode = node;
            }
        }

        if (tmpNode == null)
        {
            throw new IllegalArgumentException("Local host not found among cassandra hosts");
        }

        final Node finalNode = tmpNode;

        myNativeConnectionProvider = new NativeConnectionProvider()
        {
            @Override
            public CqlSession getSession()
            {
                return mySession;
            }

            @Override
            public Node getLocalNode()
            {
                return finalNode;
            }

            @Override
            public boolean getRemoteRouting()
            {
                return true;
            }
        };
    }

    @AfterClass
    public static void cleanupCassandra()
    {
        if (mySession != null)
        {
            mySession.close();
        }
    }

    public static NativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }
}
