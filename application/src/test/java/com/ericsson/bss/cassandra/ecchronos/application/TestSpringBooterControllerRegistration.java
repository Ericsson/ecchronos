/*
 * Copyright 2026 Telefonaktiebolaget LM Ericsson
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Import;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.web.bind.annotation.RestController;

/**
 * Guards against the failure mode where a new REST controller is added but not registered in
 * {@link SpringBooter}'s {@link Import} list. ecChronos registers controllers explicitly (not via component
 * scanning), so a missing entry silently leaves the endpoints unmapped at runtime while unit tests that construct
 * the controller directly still pass.
 */
public class TestSpringBooterControllerRegistration
{
    private static final String REST_PACKAGE = "com.ericsson.bss.cassandra.ecchronos.rest";

    @Test
    public void testAllRestControllersAreImported()
    {
        Set<Class<?>> imported = Arrays.stream(SpringBooter.class.getAnnotation(Import.class).value())
                .collect(Collectors.toSet());

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(RestController.class));

        Set<String> notImported = scanner.findCandidateComponents(REST_PACKAGE).stream()
                .map(bd -> loadClass(bd.getBeanClassName()))
                .filter(clazz -> !imported.contains(clazz))
                .map(Class::getName)
                .collect(Collectors.toSet());

        assertThat(notImported)
                .as("Every @RestController in %s must be listed in SpringBooter's @Import, otherwise its "
                        + "endpoints are never mapped at runtime", REST_PACKAGE)
                .isEmpty();
    }

    private static Class<?> loadClass(final String className)
    {
        try
        {
            return Class.forName(className);
        }
        catch (ClassNotFoundException e)
        {
            throw new IllegalStateException(e);
        }
    }
}
