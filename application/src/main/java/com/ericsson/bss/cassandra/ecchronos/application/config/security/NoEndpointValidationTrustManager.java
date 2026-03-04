/*
 * Copyright 2025 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import javax.net.ssl.SSLParameters;

public final class NoEndpointValidationTrustManager extends X509ExtendedTrustManager
{
    private static final String NO_ENDPOINT_IDENTIFICATION_ALGORITHM = "";
    private final X509ExtendedTrustManager delegateTrustManager;

    NoEndpointValidationTrustManager(final X509ExtendedTrustManager trustManager)
    {
        this.delegateTrustManager = trustManager;
    }

    @Override
    public void checkClientTrusted(
        final X509Certificate[] chain,
        final String authType,
        final SSLEngine engine) throws CertificateException
    {
        String endpointIdentificationAlgorithm = engine.getSSLParameters().getEndpointIdentificationAlgorithm();
        try
        {
            delegateTrustManager.checkClientTrusted(chain, authType, modifiedSSLEngine(engine, NO_ENDPOINT_IDENTIFICATION_ALGORITHM));
        }
        finally
        {
            modifiedSSLEngine(engine, endpointIdentificationAlgorithm);
        }
    }

    @Override
    public void checkServerTrusted(
        final X509Certificate[] chain,
        final String authType,
        final SSLEngine engine) throws CertificateException
    {
        String endpointIdentificationAlgorithm = engine.getSSLParameters().getEndpointIdentificationAlgorithm();
        try
        {
            delegateTrustManager.checkServerTrusted(chain, authType, modifiedSSLEngine(engine, NO_ENDPOINT_IDENTIFICATION_ALGORITHM));
        }
        finally
        {
            modifiedSSLEngine(engine, endpointIdentificationAlgorithm);
        }
    }

    @Override
    public void checkClientTrusted(
        final X509Certificate[] chain,
        final String authType) throws CertificateException
    {
        delegateTrustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(
        final X509Certificate[] chain,
        final String authType) throws CertificateException
    {
        delegateTrustManager.checkServerTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(
        final X509Certificate[] chain,
        final String authType,
        final Socket socket) throws CertificateException
    {
        delegateTrustManager.checkClientTrusted(chain, authType, socket);
    }

    @Override
    public void checkServerTrusted(
        final X509Certificate[] chain,
        final String authType,
        final Socket socket) throws CertificateException
    {
        delegateTrustManager.checkServerTrusted(chain, authType, socket);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
        return delegateTrustManager.getAcceptedIssuers();
    }

    private SSLEngine modifiedSSLEngine(
        final SSLEngine engine,
        final String algorithm)
    {
        SSLParameters sslParameters = engine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm(algorithm);
        engine.setSSLParameters(sslParameters);
        return engine;
    }
}
