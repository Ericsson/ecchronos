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
package com.ericsson.bss.cassandra.ecchronos.application;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockMakers;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class TestNoopStatementDecorator
{
    @Mock(mockMaker = MockMakers.SUBCLASS)
    private Statement mockStatement = Mockito.mock(Statement.class, Mockito.withSettings().mockMaker(MockMakers.SUBCLASS));

    @Test
    public void testApplyPreservesStatement()
    {
        NoopStatementDecorator noopStatementDecorator = new NoopStatementDecorator(new Config());
        assertThat(noopStatementDecorator.apply(mockStatement)).isEqualTo(mockStatement);
    }
}
