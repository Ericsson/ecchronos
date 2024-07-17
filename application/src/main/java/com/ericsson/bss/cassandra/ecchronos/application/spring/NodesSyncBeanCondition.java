/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import com.ericsson.bss.cassandra.ecchronos.application.config.Config;

@Configuration
public class NodesSyncBeanCondition implements Condition
{
    /**
     * Defines if NodesSyncBean should be created or not.
     *
     * @param context ConditionContext object
     * @param metadata AnnotatedTypeMetadata object
     * @return boolean
     */
    @Override
    public boolean matches(
        final ConditionContext context,
        final AnnotatedTypeMetadata metadata)
    {
        return context.getBeanFactory()
                        .getBean(Config.class)
                        .getConnectionConfig()
                        .getCqlConnection()
                        .getAgentConnectionConfig()
                        .isEnabled();
    }
}
