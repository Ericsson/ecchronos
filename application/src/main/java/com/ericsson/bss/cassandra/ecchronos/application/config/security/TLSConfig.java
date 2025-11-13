package com.ericsson.bss.cassandra.ecchronos.application.config.security;

import java.util.Optional;
public interface TLSConfig
{
    boolean isEnabled();

    boolean isCertificateConfigured();

    Optional<String> getCertificatePath();

    Optional<String> getCertificatePrivateKeyPath();

    Optional<String> getTrustCertificatePath();

    Optional<String[]> getCipherSuites();

    boolean requiresEndpointVerification();

    String getKeyStorePath();

    String getKeyStorePassword();

    String getTrustStorePath();

    String getTrustStorePassword();

    Optional<String> getStoreType();

    Optional<String> getAlgorithm();

    String[] getProtocols();

    CRLConfig getCRLConfig();
}
