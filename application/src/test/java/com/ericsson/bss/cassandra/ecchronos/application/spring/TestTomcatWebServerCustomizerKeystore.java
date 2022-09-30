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

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith (SpringRunner.class)
@SpringBootTest (webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.ssl.key-store=src/test/resources/server/ks.p12",
                "server.ssl.key-store-password=",
                "server.ssl.key-store-type=PKCS12",
                "server.ssl.key-alias=cert",
                "server.ssl.key-password=",
                "server.ssl.trust-store=src/test/resources/server/ts.p12",
                "server.ssl.trust-store-password=",
                "server.ssl.trust-store-type=PKCS12"
        })
@ContextConfiguration(initializers = TestTomcatWebServerCustomizer.PropertyOverrideContextInitializer.class)
public class TestTomcatWebServerCustomizerKeystore extends TestTomcatWebServerCustomizer {}