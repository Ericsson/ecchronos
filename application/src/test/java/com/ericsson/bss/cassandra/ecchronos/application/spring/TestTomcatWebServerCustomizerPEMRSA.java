/*
 * Copyright 2022 Telefonaktiebolaget LM Ericsson
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

import org.awaitility.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

@RunWith (SpringRunner.class)
@SpringBootTest (webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.ssl.certificate=src/test/resources/server-rsa/cert.crt",
                "server.ssl.certificate-private-key=src/test/resources/server-rsa/key.pem",
                "server.ssl.trust-certificate=src/test/resources/server-rsa/ca.crt",
                "metricsServer.ssl.certificate=src/test/resources/metrics-server-rsa/cert.crt",
                "metricsServer.ssl.certificate-private-key=src/test/resources/metrics-server-rsa/key.pem",
                "metricsServer.ssl.trust-certificate=src/test/resources/metrics-server-rsa/ca.crt"
        })
@ContextConfiguration(initializers = TestTomcatWebServerCustomizer.PropertyOverrideContextInitializer.class)
public class TestTomcatWebServerCustomizerPEMRSA extends TestTomcatWebServerCustomizer
{
        @Test
        public void testSuccessfulCertificateReloadingWithTrustCertificate()
        {
                await().atMost(new Duration(REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                        .untilAsserted(() -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadDefaultTrustStore());
                await().atMost(new Duration(METRICS_REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                        .untilAsserted(() -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadMetricsTrustStore());
        }
}