/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import java.util.function.Supplier;

public abstract class Connection<T>
{
    private Class<? extends T> myProviderClass;
    private Class<? extends CertificateHandler> myCertificateHandlerClass;

    @JsonProperty("provider")
    public final Class<? extends T> getProviderClass()
    {
        return myProviderClass;
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

    @JsonProperty("certificateHandler")
    public final Class<? extends CertificateHandler> getCertificateHandlerClass()
    {
        return myCertificateHandlerClass;
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
}
