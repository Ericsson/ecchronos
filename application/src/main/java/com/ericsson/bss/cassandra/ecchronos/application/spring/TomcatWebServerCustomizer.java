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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.apache.tomcat.util.net.jsse.PEMFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class TomcatWebServerCustomizer implements WebServerFactoryCustomizer<TomcatServletWebServerFactory>
{
    private static final Logger LOG = LoggerFactory.getLogger(TomcatWebServerCustomizer.class);

    private Http11NioProtocol metricsServerHttp11NioProtocol;
    private SSLHostConfig metricsSSLHostConfig;
    private Ssl metricsSsl;

    @Value("${server.ssl.enabled:false}")
    private Boolean isSslEnabled;

    @Autowired
    private MetricsServerProperties metricsServerProperties;

    @Override
    public final void customize(final TomcatServletWebServerFactory factory)
    {
        if (metricsServerProperties.isEnabled())
        {
            factory.addAdditionalTomcatConnectors(metricsConnector());
        }
    }

    private Connector metricsConnector()
    {
        Connector connector = new Connector();
        metricsSsl = metricsServerProperties.getSsl();
        boolean sslEnabled = false;
        if (metricsSsl != null)
        {
            sslEnabled = metricsSsl.isEnabled();
        }
        connector.setPort(metricsServerProperties.getPort());
        connector.setSecure(sslEnabled);
        connector.setScheme(getHttpScheme(sslEnabled));
        metricsServerHttp11NioProtocol = (Http11NioProtocol) connector.getProtocolHandler();
        if (sslEnabled)
        {
            metricsSSLHostConfig = getSslHostConfig(metricsSsl);
            metricsServerHttp11NioProtocol.addSslHostConfig(metricsSSLHostConfig);
            metricsServerHttp11NioProtocol.setSSLEnabled(true);
        }
        return connector;
    }

    private String getHttpScheme(final boolean secure)
    {
        return secure ? "https" : "http";
    }

    private SSLHostConfig getSslHostConfig(final Ssl ssl)
    {
        SSLHostConfig sslHostConfig = new SSLHostConfig();
        setCertificates(sslHostConfig, ssl);
        setTrustedCertificates(sslHostConfig, ssl);
        String[] enabledProtocols = ssl.getEnabledProtocols();
        if (enabledProtocols != null && enabledProtocols.length != 0)
        {
            sslHostConfig.setProtocols(arrayToCommaSeparated(enabledProtocols));
        }
        String[] ciphers = ssl.getCiphers();
        if (ciphers != null && ciphers.length != 0)
        {
            sslHostConfig.setCiphers(arrayToCommaSeparated(ciphers));
        }
        setAndValidateClientAuth(sslHostConfig, ssl.getTrustStore(), ssl.getTrustCertificate(), ssl.getClientAuth());
        return sslHostConfig;
    }

    private void setCertificates(final SSLHostConfig sslHostConfig, final Ssl ssl)
    {
        SSLHostConfigCertificate sslHostConfigCertificate =
                new SSLHostConfigCertificate(sslHostConfig, SSLHostConfigCertificate.Type.UNDEFINED);
        sslHostConfig.addCertificate(sslHostConfigCertificate);
        if (ssl.getCertificate() != null && ssl.getCertificatePrivateKey() != null)
        {
            sslHostConfigCertificate.setCertificateFile(getFilePath(ssl.getCertificate()));
            sslHostConfigCertificate.setCertificateKeyFile(getFilePath(ssl.getCertificatePrivateKey()));
        }
        else if (ssl.getKeyStore() != null)
        {
            sslHostConfigCertificate.setCertificateKeystoreFile(getFilePath(ssl.getKeyStore()));
            sslHostConfigCertificate.setCertificateKeystorePassword(ssl.getKeyStorePassword());
            sslHostConfigCertificate.setCertificateKeystoreType(ssl.getKeyStoreType());
            sslHostConfigCertificate.setCertificateKeyAlias(ssl.getKeyAlias());
            sslHostConfigCertificate.setCertificateKeyPassword(ssl.getKeyPassword());
        }
        else
        {
            throw new IllegalStateException("key-store or certificate must be provided if using ssl");
        }
    }

    private void setTrustedCertificates(final SSLHostConfig sslHostConfig, final Ssl ssl)
    {
        if (ssl.getTrustCertificate() != null)
        {
            sslHostConfig.setTrustStore(getTrustStore(getFilePath(ssl.getTrustCertificate())));
        }
        else if (ssl.getTrustStore() != null)
        {
            sslHostConfig.setTruststoreFile(getFilePath(ssl.getTrustStore()));
            sslHostConfig.setTruststorePassword(ssl.getTrustStorePassword());
            sslHostConfig.setTruststoreType(ssl.getTrustStoreType());
        }
    }

    private String arrayToCommaSeparated(final String... elements)
    {
        if (elements == null)
        {
            return "";
        }
        return Arrays.stream(elements).collect(Collectors.joining(","));
    }

    private void setAndValidateClientAuth(final SSLHostConfig sslHostConfig, final String trustStoreFile,
            final String trustCertificate, final Ssl.ClientAuth clientAuth)
    {
        if (Ssl.ClientAuth.NEED.equals(clientAuth))
        {
            if (trustCertificate == null && trustStoreFile == null)
            {
                throw new IllegalStateException(
                        "truststore or trust certificate must be provided if using client-auth 'need'");
            }
            sslHostConfig.setCertificateVerification(String.valueOf(SSLHostConfig.CertificateVerification.REQUIRED));
        }
        else if (Ssl.ClientAuth.WANT.equals(clientAuth))
        {
            sslHostConfig.setCertificateVerification(String.valueOf(SSLHostConfig.CertificateVerification.OPTIONAL));
        }
    }

    private String getFilePath(final String file)
    {
        return new File(file).toURI().toString();
    }

    private KeyStore getTrustStore(final String certificateAuthorities)
    {
        KeyStore trustStore = null;
        try
        {
            trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null);

            PEMFile certificateBundle = new PEMFile(certificateAuthorities);
            for (X509Certificate certificate : certificateBundle.getCertificates())
            {
                trustStore.setCertificateEntry(certificate.getSerialNumber().toString(), certificate);
            }
        }
        catch (GeneralSecurityException | IOException exception)
        {
            LOG.warn("Unable to load certificate authorities", exception);
        }
        return trustStore;
    }

    /**
     * Reload the {@code SSLHostConfig} for the metrics server if SSL is enabled.
     * Doing so should update ssl settings and fetch certificates from Keystores/PEM files.
     * It reloads them every 60 seconds by default
     */
    @Scheduled(initialDelayString = "${metricsServer.ssl.refresh-rate-in-ms:60000}",
               fixedRateString = "${metricsServer.ssl.refresh-rate-in-ms:60000}")
    public void reloadMetricsServerSslContext()
    {
        if (metricsServerHttp11NioProtocol != null && metricsSSLHostConfig != null)
        {
            if (metricsSsl.getTrustCertificate() != null)
            {
                reloadMetricsTrustStore();
            }
            metricsServerHttp11NioProtocol.reloadSslHostConfigs();
        }
    }

    /**
     * Only for tests.
     */
    void setMetricsPortProperty()
    {
        if (metricsServerHttp11NioProtocol != null)
        {
            System.setProperty("metricsServer.local.port",
                    String.valueOf(metricsServerHttp11NioProtocol.getLocalPort()));
        }
    }

    /**
     * Reloads metrics server trustStore.
     */
    void reloadMetricsTrustStore()
    {
        metricsSSLHostConfig.setTrustStore(getTrustStore(getFilePath(metricsSsl.getTrustCertificate())));
    }
}
