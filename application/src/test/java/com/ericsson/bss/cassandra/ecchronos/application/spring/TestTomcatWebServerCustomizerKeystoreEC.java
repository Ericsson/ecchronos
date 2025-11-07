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

import static org.springframework.test.context.support.TestPropertySourceUtils.addInlinedPropertiesToEnvironment;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import com.ericsson.bss.cassandra.ecchronos.application.utils.CertUtils;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = TestTomcatWebServerCustomizerKeystoreEC.PropertyOverrideContextInitializer.class)
public class TestTomcatWebServerCustomizerKeystoreEC extends TestTomcatWebServerCustomizer
{
    @BeforeClass
    public static void setup()
    {
        createCerts(CertUtils.EC_ALGORITHM_NAME, true);
    }

    static class PropertyOverrideContextInitializer
            extends GlobalPropertyOverrideContextInitializer
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
                    "metricsServer.ssl.key-store=" + metricsServerKeyStore,
                    "metricsServer.ssl.key-store-password=" + KEYSTORE_PASSWORD,
                    "metricsServer.ssl.key-store-type=PKCS12",
                    "metricsServer.ssl.key-alias=cert",
                    "metricsServer.ssl.key-password=" + KEYSTORE_PASSWORD,
                    "metricsServer.ssl.trust-store=" + metricsServerTrustStore,
                    "metricsServer.ssl.trust-store-password=" + KEYSTORE_PASSWORD,
                    "metricsServer.ssl.trust-store-type=PKCS12");
        }
    }
}
