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

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.ssl.key-store=src/test/resources/server-ec/ks.p12",
                "server.ssl.key-store-password=ecctest",
                "server.ssl.key-store-type=PKCS12",
                "server.ssl.key-alias=1",
                "server.ssl.key-password=ecctest",
                "server.ssl.trust-store=src/test/resources/server-ec/ts.p12",
                "server.ssl.trust-store-password=ecctest",
                "server.ssl.trust-store-type=PKCS12",
                "metricsServer.ssl.key-store=src/test/resources/metrics-server-ec/ks.p12",
                "metricsServer.ssl.key-store-password=ecctest",
                "metricsServer.ssl.key-store-type=PKCS12",
                "metricsServer.ssl.key-alias=1",
                "metricsServer.ssl.key-password=ecctest",
                "metricsServer.ssl.trust-store=src/test/resources/metrics-server-ec/ts.p12",
                "metricsServer.ssl.trust-store-password=ecctest",
                "metricsServer.ssl.trust-store-type=PKCS12",
        })
@ContextConfiguration(initializers = TestTomcatWebServerCustomizer.PropertyOverrideContextInitializer.class)
public class TestTomcatWebServerCustomizerKeystoreEC extends TestTomcatWebServerCustomizer
{
        @Before
        public void setup()
        {
                clientValidPath = "valid-ec/";
                clientExpiredPath = "expired-ec/";
                clientPassword = "ecctest";
                metricsClientValidPath = "metrics-valid-ec/";
                metricsClientExpiredPath = "metrics-expired-ec/";
                metricsClientPassword = "ecctest";
        }
}
