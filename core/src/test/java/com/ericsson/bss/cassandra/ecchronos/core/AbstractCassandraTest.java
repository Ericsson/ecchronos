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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.datastax.driver.core.Host;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.ericsson.bss.cassandra.ecchronos.connection.NativeConnectionProvider;

import net.jcip.annotations.NotThreadSafe;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;

@NotThreadSafe
public abstract class AbstractCassandraTest
{
    protected static CassandraDaemonForECChronos myCassandraDaemon;

    protected static Cluster myCluster;
    protected static Session mySession;

    private static NativeConnectionProvider myNativeConnectionProvider;

    @BeforeClass
    public static void setupCassandra() throws IOException
    {
        myCassandraDaemon = CassandraDaemonForECChronos.getInstance();

        myCluster = myCassandraDaemon.getCluster();
        myCluster.getConfiguration().getCodecRegistry().register(SimpleTimestampCodec.instance);
        mySession = myCluster.connect();

        Host tmpHost = null;

        try
        {
            InetAddress localhostAddress = InetAddress.getByName("localhost");

            for (Host host : myCluster.getMetadata().getAllHosts())
            {
                if (host.getAddress().equals(localhostAddress))
                {
                    tmpHost = host;
                }
            }
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException(e);
        }

        if (tmpHost == null)
        {
            throw new IllegalArgumentException("Local host not found among cassandra hosts");
        }

        final Host finalHost = tmpHost;

        myNativeConnectionProvider = new NativeConnectionProvider()
        {
            @Override
            public Session getSession()
            {
                return mySession;
            }

            @Override
            public Host getLocalHost()
            {
                return finalHost;
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
        mySession.close();
        myCluster.close();
    }

    public static NativeConnectionProvider getNativeConnectionProvider()
    {
        return myNativeConnectionProvider;
    }
}
