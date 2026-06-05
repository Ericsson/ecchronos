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

import com.ericsson.bss.cassandra.ecchronos.application.config.RetryPolicy;
import com.ericsson.bss.cassandra.ecchronos.connection.CertificateHandler;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Base class for connection configuration.
 * @param <T> the type parameter
 */
public abstract class Connection<T>
{
    private String myHost = "localhost";
    private int myPort;
    private Class<? extends T> myProviderClass;
    private Class<? extends CertificateHandler> myCertificateHandlerClass;
    private Timeout myTimeout = new Timeout();
    private RetryPolicy myRetryPolicy = new RetryPolicy();

    /** Default constructor. */
    protected Connection()
    {
    }

    /**
     * Returns the host.
     * @return the host
     */
    @JsonProperty("host")
    public final String getHost()
    {
        return myHost;
    }

    /**
     * Returns the port.
     * @return the port
     */
    @JsonProperty("port")
    public final int getPort()
    {
        return myPort;
    }

    /**
     * Returns the provider class.
     * @return the provider class
     */
    @JsonProperty("provider")
    public final Class<? extends T> getProviderClass()
    {
        return myProviderClass;
    }

    /**
     * Returns the certificate handler class.
     * @return the certificate handler class
     */
    @JsonProperty("certificateHandler")
    public final Class<? extends CertificateHandler> getCertificateHandlerClass()
    {
        return myCertificateHandlerClass;
    }

    /**
     * Returns the timeout.
     * @return the timeout
     */
    @JsonProperty("timeout")
    public final Timeout getTimeout()
    {
        return myTimeout;
    }

    /**
     * Returns the retry policy.
     * @return the retry policy
     */
    @JsonProperty("retryPolicy")
    public final RetryPolicy getRetryPolicy()
    {
        return myRetryPolicy;
    }

    /**
     * Sets the host.
     * @param host the hostname
     */
    @JsonProperty("host")
    public final void setHost(final String host)
    {
        myHost = host;
    }

    /**
     * Sets the port.
     * @param port the port number
     */
    @JsonProperty("port")
    public final void setPort(final int port)
    {
        myPort = port;
    }

    /**
     * Sets the provider.
     * @param providerClass the provider class
     * @throws NoSuchMethodException if the method is not found
     */
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
    public final void setCertificateHandler(final Class<? extends CertificateHandler> certificateHandlerClass)
            throws NoSuchMethodException
    {
        certificateHandlerClass.getDeclaredConstructor(expectedCertHandlerConstructor());

        myCertificateHandlerClass = certificateHandlerClass;
    }

    /**
     * Sets the timeout.
     * @param timeout the timeout duration
     */
    @JsonProperty("timeout")
    public final void setTimeout(final Timeout timeout)
    {
        myTimeout = timeout;
    }

    /**
     * Sets the retry count.
     * @param retryPolicy the retry policy
     */
    @JsonProperty("retryPolicy")
    public final void setRetryCount(final RetryPolicy retryPolicy)
    {
        myRetryPolicy = retryPolicy;
    }

    /**
     * Returns the expected constructor signature.
     * @return the expected constructor parameter types
     */
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

    /** Handles a lock acquisition timeout. */
    public static class Timeout
    {
        private long myTime = 0;
        private TimeUnit myTimeUnit = TimeUnit.MILLISECONDS;

        /** Default constructor. */
        public Timeout()
        {
        }

        /**
         * Returns the connection timeout.
         * @param timeUnit the time unit
         * @return the connection timeout
         */
        public final long getConnectionTimeout(final TimeUnit timeUnit)
        {
            return timeUnit.convert(myTime, myTimeUnit);
        }

        /**
         * Sets the time.
         * @param time the timestamp
         */
        @JsonProperty("time")
        public final void setTime(final long time)
        {
            myTime = time;
        }

        /**
         * Sets the time unit.
         * @param timeUnit the time unit
         */
        @JsonProperty("unit")
        public final void setTimeUnit(final String timeUnit)
        {
            myTimeUnit = TimeUnit.valueOf(timeUnit.toUpperCase(Locale.US));
        }
    }
}
