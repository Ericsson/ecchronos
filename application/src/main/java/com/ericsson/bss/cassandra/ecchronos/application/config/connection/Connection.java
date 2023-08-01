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
package com.ericsson.bss.cassandra.ecchronos.application.config.connection;

import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class Connection<T>
{
    private String myHost = "localhost";
    private int myPort;
    private Class<? extends T> myProviderClass;
    private Class<? extends CertificateHandler> myCertificateHandlerClass;
    private Timeout myTimeout = new Timeout();

    @JsonProperty("host")
    public final String getHost()
    {
        return myHost;
    }

    @JsonProperty("port")
    public final int getPort()
    {
        return myPort;
    }

    @JsonProperty("provider")
    public final Class<? extends T> getProviderClass()
    {
        return myProviderClass;
    }

    @JsonProperty("certificateHandler")
    public final Class<? extends CertificateHandler> getCertificateHandlerClass()
    {
        return myCertificateHandlerClass;
    }

    @JsonProperty("timeout")
    public final Timeout getTimeout()
    {
        return myTimeout;
    }

    @JsonProperty("host")
    public final void setHost(final String host)
    {
        myHost = host;
    }

    @JsonProperty("port")
    public final void setPort(final int port)
    {
        myPort = port;
    }

    @JsonProperty("provider")
    public final void setProvider(final Class<? extends T> providerClass) throws NoSuchMethodException
    {
        providerClass.getDeclaredConstructor(expectedConstructor());

        myProviderClass = providerClass;
    }

    /**
     * Set certification handler.
     *
     * @param certificateHandlerClass
     *         The certification handler.
     * @throws NoSuchMethodException
     *         If the getDeclaredConstructor method was not found.
     */
    @JsonProperty("certificateHandler")
    public void setCertificateHandler(final Class<? extends CertificateHandler> certificateHandlerClass)
            throws NoSuchMethodException
    {
        certificateHandlerClass.getDeclaredConstructor(expectedCertHandlerConstructor());

        myCertificateHandlerClass = certificateHandlerClass;
    }

    @JsonProperty("timeout")
    public final void setTimeout(final Timeout timeout)
    {
        myTimeout = timeout;
    }

    protected abstract Class<?>[] expectedConstructor();

    /**
     * Get the expected certification handler.
     *
     * @return CertificateHandler
     */
    protected Class<?>[] expectedCertHandlerConstructor()
    {
        return new Class<?>[]
                {
                        Supplier.class
                };
    }

    /**
     * String representation.
     *
     * @return String
     */
    @Override
    public String toString()
    {
        return String.format("(%s:%d:%s),provider=%s,certificateHandler=%s",
                myHost, myPort, myTimeout, myProviderClass, myCertificateHandlerClass);
    }

    public static class Timeout
    {
        private long myTime = 0;
        private TimeUnit myTimeUnit = TimeUnit.MILLISECONDS;

        public final long getConnectionTimeout(final TimeUnit timeUnit)
        {
            return timeUnit.convert(myTime, myTimeUnit);
        }

        @JsonProperty("time")
        public final void setTime(final long time)
        {
            myTime = time;
        }

        @JsonProperty("unit")
        public final void setTimeUnit(final String timeUnit)
        {
            myTimeUnit = TimeUnit.valueOf(timeUnit.toUpperCase(Locale.US));
        }
    }
}
