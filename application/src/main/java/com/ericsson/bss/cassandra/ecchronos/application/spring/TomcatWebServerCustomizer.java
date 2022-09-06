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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
@SuppressWarnings({})
public class TomcatWebServerCustomizer implements WebServerFactoryCustomizer<TomcatServletWebServerFactory>
{
    private Http11NioProtocol http11NioProtocol;

    @Value("${server.ssl.enabled:false}")
    private Boolean sslIsEnabled;

    @Override
    public final void customize(final TomcatServletWebServerFactory factory)
    {
        if (sslIsEnabled)
        {
            factory.addConnectorCustomizers(connector ->
            {
                http11NioProtocol = (Http11NioProtocol) connector.getProtocolHandler();
            });
        }
    }

    /**
     * Reload the {@code SSLHostConfig} if SSL is enabled. Doing so should update ssl settings and fetch
     * certificates from Keystores. It reloads them every 60 seconds by default
     */
    @Scheduled (initialDelayString = "${server.ssl.refresh-rate-in-ms:60000}",
            fixedRateString = "${server.ssl.refresh-rate-in-ms:60000}")
    public void reloadSslContext()
    {
        if (sslIsEnabled && http11NioProtocol != null)
        {
            http11NioProtocol.reloadSslHostConfigs();
        }
    }
}
