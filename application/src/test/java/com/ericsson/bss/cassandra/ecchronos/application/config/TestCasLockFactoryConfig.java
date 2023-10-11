/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import com.ericsson.bss.cassandra.ecchronos.application.config.lockfactory.CasLockFactoryConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCasLockFactoryConfig
{
    private static CasLockFactoryConfig casLockFactoryConfig;

    @Before
    public void setup() throws IOException
    {
        if (casLockFactoryConfig == null)
        {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            File file = new File(classLoader.getResource("all_set.yml").getFile());
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Config config = mapper.readValue(file, Config.class);
            casLockFactoryConfig = config.getLockFactory().getCasLockFactoryConfig();

        }
    }

    @Test
    public void testKeyspaceNameFromYaml()
    {
        assertThat(casLockFactoryConfig.getKeyspaceName()).isEqualTo("ecc");
    }

    @Test
    public void testLockTimeInSecondsFromYaml()
    {
        assertThat(casLockFactoryConfig.getLockTimeInSeconds()).isEqualTo(800L);
    }

    @Test
    public void testLockUpdateTimeInSecondsFromYaml()
    {
        assertThat(casLockFactoryConfig.getLockUpdateTimeInSeconds()).isEqualTo(80L);
    }

    @Test
    public void testExpiryTimeInSecondsFromYaml()
    {
        assertThat(casLockFactoryConfig.getFailureCacheExpiryTimeInSeconds()).isEqualTo(100L);
    }
}
