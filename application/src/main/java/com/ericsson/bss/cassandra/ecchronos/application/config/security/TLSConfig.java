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

import java.util.Optional;

/**
 * Configuration interface for TLS/SSL settings used to secure connections.
 * Provides access to certificate paths, key store and trust store settings,
 * cipher suites, protocols, and CRL configuration.
 */
public interface TLSConfig
{
    /**
     * Checks whether TLS is enabled.
     *
     * @return {@code true} if TLS is enabled, {@code false} otherwise.
     */
    boolean isEnabled();

    /**
     * Checks whether PEM-based certificate configuration is present.
     *
     * @return {@code true} if certificate paths are configured, {@code false} otherwise.
     */
    boolean isCertificateConfigured();

    /**
     * Returns the path to the PEM certificate file.
     *
     * @return an optional containing the certificate path, or empty if not configured.
     */
    Optional<String> getCertificatePath();

    /**
     * Returns the path to the PEM private key file for the certificate.
     *
     * @return an optional containing the private key path, or empty if not configured.
     */
    Optional<String> getCertificatePrivateKeyPath();

    /**
     * Returns the path to the PEM trust certificate file.
     *
     * @return an optional containing the trust certificate path, or empty if not configured.
     */
    Optional<String> getTrustCertificatePath();

    /**
     * Returns the cipher suites to use for TLS connections.
     *
     * @return an optional containing the array of cipher suite names, or empty if not configured.
     */
    Optional<String[]> getCipherSuites();

    /**
     * Checks whether endpoint verification (hostname verification) is required.
     *
     * @return {@code true} if endpoint verification is required, {@code false} otherwise.
     */
    boolean requiresEndpointVerification();

    /**
     * Returns the path to the key store file.
     *
     * @return the key store file path.
     */
    String getKeyStorePath();

    /**
     * Returns the password for the key store.
     *
     * @return the key store password.
     */
    String getKeyStorePassword();

    /**
     * Returns the path to the trust store file.
     *
     * @return the trust store file path.
     */
    String getTrustStorePath();

    /**
     * Returns the password for the trust store.
     *
     * @return the trust store password.
     */
    String getTrustStorePassword();

    /**
     * Returns the store type (e.g. JKS, PKCS12).
     *
     * @return an optional containing the store type, or empty if not configured.
     */
    Optional<String> getStoreType();

    /**
     * Returns the algorithm used by the key manager and trust manager factories.
     *
     * @return an optional containing the algorithm name, or empty if not configured.
     */
    Optional<String> getAlgorithm();

    /**
     * Returns the TLS protocol versions to enable.
     *
     * @return an array of protocol version strings.
     */
    String[] getProtocols();

    /**
     * Returns the CRL (Certificate Revocation List) configuration.
     *
     * @return the CRL configuration.
     */
    CRLConfig getCRLConfig();
}
