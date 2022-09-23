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
package com.ericsson.bss.cassandra.ecchronos.application.spring;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.jsse.PEMFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.Ssl;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

@Component
@EnableScheduling
@SuppressWarnings("PMD.TooManyFields")
public class TomcatWebServerCustomizer implements WebServerFactoryCustomizer<TomcatServletWebServerFactory>
{
    private static final Logger LOG = LoggerFactory.getLogger(TomcatWebServerCustomizer.class);

    private Http11NioProtocol defaultServerHttp11NioProtocol;
    private Http11NioProtocol metricsServerHttp11NioProtocol;
    private SSLHostConfig defaultSSLHostConfig;
    private SSLHostConfig metricsSSLHostConfig;

    @Value("${server.ssl.enabled:false}")
    private Boolean isSslEnabled;

    @Value("${server.ssl.key-store:#{null}}")
    private String serverKeyStoreFile;

    @Value("${server.ssl.key-store-password:#{null}}")
    private String serverKeyStorePassword;

    @Value("${server.ssl.key-store-type:JKS}")
    private String serverKeyStoreType;

    @Value("${server.ssl.key-alias:cert}")
    private String serverKeyAlias;

    @Value("${server.ssl.key-password:#{null}}")
    private String serverKeyPassword;

    @Value("${server.ssl.trust-store:#{null}}")
    private String serverTrustStoreFile;

    @Value("${server.ssl.trust-store-password:#{null}}")
    private String serverTrustStorePassword;

    @Value("${server.ssl.trust-store-type:JKS}")
    private String serverTrustStoreType;

    @Value("${server.ssl.certificate:#{null}}")
    private String serverCertificate;

    @Value("${server.ssl.certificate-private-key:#{null}}")
    private String serverCertificatePrivateKey;

    @Value("${server.ssl.trust-certificate:#{null}}")
    private String serverTrustCertificate;

    @Value("${server.ssl.client-auth:none}")
    private String serverClientAuth;

    @Value("${server.ssl.enabled-protocols:TLSv1.2}")
    private String serverEnabledProtocols;

    @Value("${server.ssl.ciphers:#{null}}")
    private String serverCiphers;

    @Value("${metricsServer.enabled:false}")
    private Boolean isMetricsServerEnabled;

    @Value("${metricsServer.ssl.enabled:false}")
    private Boolean isMetricsSslEnabled;

    @Value("${metricsServer.ssl.key-store:#{null}}")
    private String metricsServerKeyStoreFile;

    @Value("${metricsServer.ssl.key-store-password:#{null}}")
    private String metricsServerKeyStorePassword;

    @Value("${metricsServer.ssl.key-store-type:JKS}")
    private String metricsServerKeyStoreType;

    @Value("${metricsServer.ssl.key-alias:cert}")
    private String metricsServerKeyAlias;

    @Value("${metricsServer.ssl.key-password:#{null}}")
    private String metricsServerKeyPassword;

    @Value("${metricsServer.ssl.trust-store:#{null}}")
    private String metricsServerTrustStoreFile;

    @Value("${metricsServer.ssl.trust-store-password:#{null}}")
    private String metricsServerTrustStorePassword;

    @Value("${metricsServer.ssl.trust-store-type:JKS}")
    private String metricsServerTrustStoreType;

    @Value("${metricsServer.ssl.certificate:#{null}}")
    private String metricsServerCertificate;

    @Value("${metricsServer.ssl.certificate-private-key:#{null}}")
    private String metricsServerCertificatePrivateKey;

    @Value("${metricsServer.ssl.trust-certificate:#{null}}")
    private String metricsServerTrustCertificate;

    @Value("${metricsServer.ssl.client-auth:none}")
    private String metricsServerClientAuth;

    @Value("${metricsServer.ssl.enabled-protocols:TLSv1.2}")
    private String metricsServerEnabledProtocols;

    @Value("${metricsServer.ssl.ciphers:#{null}}")
    private String metricsServerCiphers;

    @Value("${metricsServer.port:8081}")
    private int metricsServerPort;

    @Override
    public final void customize(final TomcatServletWebServerFactory factory)
    {
        //Disable ssl on factory level, ssl will be configured for each connector instead.
        Ssl ssl = factory.getSsl();
        if (ssl != null)
        {
            ssl.setEnabled(false);
        }
        if (isSslEnabled)
        {
            factory.addConnectorCustomizers(connector ->
            {
                defaultServerHttp11NioProtocol = (Http11NioProtocol) connector.getProtocolHandler();
                defaultSSLHostConfig = getSslHostConfig(serverKeyStoreFile, serverKeyStorePassword, serverKeyStoreType,
                        serverKeyAlias, serverKeyPassword, serverTrustStoreFile, serverTrustStorePassword,
                        serverTrustStoreType, serverCertificate, serverCertificatePrivateKey, serverTrustCertificate,
                        serverEnabledProtocols, serverCiphers, serverClientAuth);
                defaultServerHttp11NioProtocol.addSslHostConfig(defaultSSLHostConfig);
                defaultServerHttp11NioProtocol.setSSLEnabled(true);
            });
        }
        if (isMetricsServerEnabled)
        {
            factory.addAdditionalTomcatConnectors(metricsConnector());
        }
    }

    private Connector metricsConnector()
    {
        Connector connector = new Connector();
        connector.setPort(metricsServerPort);
        connector.setSecure(isMetricsSslEnabled);
        connector.setScheme(getHttpScheme(isMetricsSslEnabled));
        metricsServerHttp11NioProtocol = (Http11NioProtocol) connector.getProtocolHandler();
        if (isMetricsSslEnabled)
        {
            metricsSSLHostConfig = getSslHostConfig(metricsServerKeyStoreFile, metricsServerKeyStorePassword,
                    metricsServerKeyStoreType, metricsServerKeyAlias, metricsServerKeyPassword,
                    metricsServerTrustStoreFile, metricsServerTrustStorePassword, metricsServerTrustStoreType,
                    metricsServerCertificate, metricsServerCertificatePrivateKey, metricsServerTrustCertificate,
                    metricsServerEnabledProtocols, metricsServerCiphers, metricsServerClientAuth);
            metricsServerHttp11NioProtocol.addSslHostConfig(metricsSSLHostConfig);
            metricsServerHttp11NioProtocol.setSSLEnabled(true);
        }
        return connector;
    }

    private String getHttpScheme(final boolean secure)
    {
        return secure ? "https" : "http";
    }

    @SuppressWarnings({ "PMD.ExcessiveParameterList", "checkstyle:ParameterNumber" })
    private SSLHostConfig getSslHostConfig(
            final String keystoreFile, final String keyStorePassword, final String keyStoreType, final String keyAlias,
            final String keyPassword, final String trustStoreFile, final String trustStorePassword,
            final String trustStoreType, final String certificate, final String certificatePrivateKey,
            final String trustCertificate, final String enabledProtocols, final String ciphers, final String clientAuth)
    {
        SSLHostConfig sslHostConfig = new SSLHostConfig();
        setCertificates(sslHostConfig, keystoreFile, keyStorePassword, keyStoreType, keyAlias, keyPassword, certificate,
                certificatePrivateKey);
        setTrustedCertificates(sslHostConfig, trustStoreFile, trustStorePassword, trustStoreType, trustCertificate);
        sslHostConfig.setProtocols(enabledProtocols);
        if (ciphers != null)
        {
            sslHostConfig.setCiphers(ciphers);
        }
        setAndValidateClientAuth(sslHostConfig, trustStoreFile, trustCertificate, clientAuth);
        return sslHostConfig;
    }

    private void setCertificates(final SSLHostConfig sslHostConfig, final String keystoreFile,
            final String keyStorePassword, final String keyStoreType, final String keyAlias, final String keyPassword,
            final String certificate, final String certificatePrivateKey)
    {
        if (certificate != null && certificatePrivateKey != null)
        {
            sslHostConfig.setCertificateFile(getFilePath(certificate));
            sslHostConfig.setCertificateKeyFile(getFilePath(certificatePrivateKey));
        }
        else if (keystoreFile != null)
        {
            sslHostConfig.setCertificateKeystoreFile(getFilePath(keystoreFile));
            sslHostConfig.setCertificateKeystorePassword(keyStorePassword);
            sslHostConfig.setCertificateKeystoreType(keyStoreType);
            sslHostConfig.setCertificateKeyAlias(keyAlias);
            sslHostConfig.setCertificateKeyPassword(keyPassword);
        }
        else
        {
            throw new IllegalStateException("key-store or certificate must be provided if using ssl.enabled 'true'");
        }
    }

    private void setTrustedCertificates(final SSLHostConfig sslHostConfig, final String trustStoreFile,
            final String trustStorePassword, final String trustStoreType, final String trustCertificate)
    {
        if (trustCertificate != null)
        {
            sslHostConfig.setTrustStore(getTrustStore(getFilePath(trustCertificate)));
        }
        else if (trustStoreFile != null)
        {
            sslHostConfig.setTruststoreFile(getFilePath(trustStoreFile));
            sslHostConfig.setTruststorePassword(trustStorePassword);
            sslHostConfig.setTruststoreType(trustStoreType);
        }
    }

    private void setAndValidateClientAuth(final SSLHostConfig sslHostConfig, final String trustStoreFile,
            final String trustCertificate, final String clientAuth)
    {
        if ("need".equalsIgnoreCase(clientAuth))
        {
            if (trustCertificate == null && trustStoreFile == null)
            {
                throw new IllegalStateException(
                        "truststore or trust certificate must be provided if using client-auth 'need'");
            }
            sslHostConfig.setCertificateVerification(String.valueOf(SSLHostConfig.CertificateVerification.REQUIRED));
        }
        else if ("want".equalsIgnoreCase(clientAuth))
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
     * Reload the {@code SSLHostConfig} for the default server if SSL is enabled.
     * Doing so should update ssl settings and fetch certificates from Keystores/PEM files.
     * It reloads them every 60 seconds by default
     */
    @Scheduled(initialDelayString = "${server.ssl.refresh-rate-in-ms:60000}",
               fixedRateString = "${server.ssl.refresh-rate-in-ms:60000}")
    public void reloadSslContext()
    {
        if (isSslEnabled && defaultServerHttp11NioProtocol != null && defaultSSLHostConfig != null)
        {
            if (serverTrustCertificate != null)
            {
                reloadDefaultTrustStore();
            }
            defaultServerHttp11NioProtocol.reloadSslHostConfigs();
        }
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
        if (isMetricsSslEnabled && metricsServerHttp11NioProtocol != null && metricsSSLHostConfig != null)
        {
            if (metricsServerTrustCertificate != null)
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
     * Reloads default server trustStore.
     */
    void reloadDefaultTrustStore()
    {
        defaultSSLHostConfig.setTrustStore(getTrustStore(getFilePath(serverTrustCertificate)));
    }

    /**
     * Reloads metrics server trustStore.
     */
    void reloadMetricsTrustStore()
    {
        metricsSSLHostConfig.setTrustStore(getTrustStore(getFilePath(metricsServerTrustCertificate)));
    }
}
