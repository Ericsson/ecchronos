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


import org.apache.coyote.http11.Http11NioProtocol;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;
import org.apache.tomcat.util.net.jsse.PEMFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

@Component
@EnableScheduling
public class TomcatWebServerCustomizer implements WebServerFactoryCustomizer<TomcatServletWebServerFactory>
{
    private Http11NioProtocol http11NioProtocol;

    @Value("${server.ssl.enabled:false}")
    private Boolean sslIsEnabled;

    @Value("${server.ssl.certificate:#{null}}")
    private String certificate;

    @Value("${server.ssl.certificate-key:#{null}}")
    private String certificateKey;

    @Value("${server.ssl.certificate-authorities:#{null}}")
    private String certificateAuthorities;

    @Value("${server.ssl.client-auth:none}")
    private String clientAuth;

    private static final Logger LOG = LoggerFactory.getLogger(TomcatWebServerCustomizer.class);

    @Override
    public void customize(TomcatServletWebServerFactory factory)
    {
        if (sslIsEnabled)
        {
            if (certificate != null)
            {
                factory.getSsl().setEnabled(false);
            }

            factory.addConnectorCustomizers(connector ->
            {
                http11NioProtocol = (Http11NioProtocol) connector.getProtocolHandler();
                if (certificate != null) {
                    http11NioProtocol.addSslHostConfig(getSslHostConfiguration());
                    http11NioProtocol.setSSLEnabled(true);
                }
            });
        }
    }

    private SSLHostConfig getSslHostConfiguration()
    {
        SSLHostConfig sslHostConfig = new SSLHostConfig();
        SSLHostConfigCertificate certificateConfig = new SSLHostConfigCertificate(sslHostConfig, SSLHostConfigCertificate.DEFAULT_TYPE);
        certificateConfig.setCertificateFile(certificate);
        certificateConfig.setCertificateKeyFile(certificateKey);
        sslHostConfig.addCertificate(certificateConfig);
        sslHostConfig.setTrustStore(getTrustStore());
        sslHostConfig.setCertificateVerification("need".equals(clientAuth) ? "true" : "false");
        return sslHostConfig;
    }

    private KeyStore getTrustStore()
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
        catch(GeneralSecurityException | IOException exception)
        {
            LOG.warn("Unable to load certificate authorities", exception);
        }
        return trustStore;
    }

    /**
     * Reload the {@code SSLHostConfig} if SSL is enabled. Doing so should update ssl settings and reload certificates
     * It reloads them every 60 seconds by default
     */
    @Scheduled (initialDelayString = "${server.ssl.refresh-rate-in-ms:60000}", fixedRateString = "${server.ssl.refresh-rate-in-ms:60000}")
    public void reloadSslContext()
    {
        if (sslIsEnabled && http11NioProtocol != null)
        {
            http11NioProtocol.reloadSslHostConfigs();
        }
    }
}
