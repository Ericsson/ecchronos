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

import com.ericsson.bss.cassandra.ecchronos.application.utils.CertUtils;
import org.awaitility.Duration;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = TestTomcatWebServerCustomizerPEMRSA.PropertyOverrideContextInitializer.class)
public class TestTomcatWebServerCustomizerPEMRSA extends TestTomcatWebServerCustomizer
{
    @BeforeClass
    public static void setup()
    {
        createCerts(CertUtils.RSA_ALGORITHM_NAME, false);
    }

    @Test
    public void testSuccessfulCertificateReloadingWithTrustCertificate()
    {
        await().atMost(new Duration(REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS)).untilAsserted(
                () -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadDefaultTrustStore());
        await().atMost(new Duration(METRICS_REFRESH_RATE * (INVOCATION_COUNT + 10), TimeUnit.MILLISECONDS))
                .untilAsserted(
                        () -> verify(tomcatWebServerCustomizer, atLeast(INVOCATION_COUNT)).reloadMetricsTrustStore());
    }

    static class PropertyOverrideContextInitializer
            extends TestTomcatWebServerCustomizer.GlobalPropertyOverrideContextInitializer
    {

        PropertyOverrideContextInitializer() throws IOException
        {
            super();
        }

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext)
        {
            super.initialize(configurableApplicationContext);
            addInlinedPropertiesToEnvironment(configurableApplicationContext,
                    "server.ssl.certificate=" + serverCert,
                    "server.ssl.certificate-private-key=" + serverCertKey,
                    "server.ssl.trust-certificate=" + clientCaCert,
                    "metricsServer.ssl.certificate=" + metricsServerCert,
                    "metricsServer.ssl.certificate-private-key=" + metricsServerCertKey,
                    "metricsServer.ssl.trust-certificate=" + metricsClientCaCert);
        }
    }
}