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
package com.ericsson.bss.cassandra.ecchronos.application;

import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.ConfigurationException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestReflectionUtils
{
    @Test
    public void testResolveAndConstructNewInstanceWithPrimitiveConstructor() throws ConfigurationException
    {
        Class<? extends TestInterface> clazz = ReflectionUtils.resolveClassOfType(OverridingClassWithPrimitiveConstructor.class.getName(), TestInterface.class, Integer.TYPE);

        TestInterface testInterface = ReflectionUtils.construct(clazz, new Class<?>[]{Integer.TYPE} , 1);

        assertThat(testInterface.getValue()).isEqualTo(1);
        assertThat(testInterface.getClass()).isEqualTo(OverridingClassWithPrimitiveConstructor.class);
    }

    @Test
    public void testResolveAndConstructNewInstanceWithObjectConstructor() throws ConfigurationException
    {
        Class<? extends TestInterface> clazz = ReflectionUtils.resolveClassOfType(OverridingClassWithObjectConstructor.class.getName(), TestInterface.class, Integer.class);

        TestInterface testInterface = ReflectionUtils.construct(clazz, 2);

        assertThat(testInterface.getValue()).isEqualTo(2);
        assertThat(testInterface.getClass()).isEqualTo(OverridingClassWithObjectConstructor.class);
    }

    @Test
    public void testResolveAndConstructNewInstanceWithoutConstructor() throws ConfigurationException
    {
        Class<? extends TestInterface> clazz = ReflectionUtils.resolveClassOfType(OverridingClassWithoutConstructor.class.getName(), TestInterface.class);

        TestInterface testInterface = ReflectionUtils.construct(clazz);

        assertThat(testInterface.getValue()).isEqualTo(OverridingClassWithoutConstructor.VALUE);
        assertThat(testInterface.getClass()).isEqualTo(OverridingClassWithoutConstructor.class);
    }

    @Test
    public void testResolveWithFaultyClassName()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.resolveClassOfType("NonExistingClass", TestInterface.class));
    }

    @Test
    public void testResolveWithFaultyConstructor()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.resolveClassOfType(OverridingClassWithoutConstructor.class.getName(), TestInterface.class, String.class));
    }

    @Test
    public void testResolveNonOverridingClass()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.resolveClassOfType(NonOverridingClass.class.getName(), TestInterface.class));
    }

    @Test
    public void testConstructClassWithPrivateConstructor()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.construct(ClassWithPrivateConstructor.class));
    }

    @Test
    public void testConstructAbstractClass()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.construct(AbstractClass.class));
    }

    @Test
    public void testConstructWithThrowingConstructor()
    {
        assertThatExceptionOfType(ConfigurationException.class)
                .isThrownBy(() -> ReflectionUtils.construct(ThrowingConstructor.class));
    }

    interface TestInterface
    {
        int getValue();
    }

    static class OverridingClassWithoutConstructor implements TestInterface
    {
        static final int VALUE = 3;

        @Override
        public int getValue()
        {
            return VALUE;
        }
    }

    static class OverridingClassWithPrimitiveConstructor implements TestInterface
    {
        private final int myValue;

        public OverridingClassWithPrimitiveConstructor(int value)
        {
            myValue = value;
        }

        @Override
        public int getValue()
        {
            return myValue;
        }
    }

    static class OverridingClassWithObjectConstructor implements TestInterface
    {
        private final int myValue;

        public OverridingClassWithObjectConstructor(Integer value)
        {
            myValue = value;
        }

        @Override
        public int getValue()
        {
            return myValue;
        }
    }

    static class NonOverridingClass
    {
    }

    static class ClassWithPrivateConstructor
    {
        private ClassWithPrivateConstructor()
        {
        }
    }

    static abstract class AbstractClass
    {
    }

    static class ThrowingConstructor
    {
        public ThrowingConstructor()
        {
            throw new IllegalStateException();
        }
    }
}
