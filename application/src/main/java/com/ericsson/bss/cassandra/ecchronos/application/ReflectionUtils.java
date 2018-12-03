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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public final class ReflectionUtils
{
    private ReflectionUtils()
    {
    }

    /**
     * Instantiate a subclass of &lt;T&gt; with the provided parameters.
     *
     * This method should only be used for non-primitive parameter types.
     *
     * @param type The type of the subclass.
     * @param parameters The parameters to send to the constructor.
     * @param <T> The interface/class to construct a subclass for.
     * @return The constructed object.
     * @throws ConfigurationException Thrown if the constructor is invalid or if it's not possible to access the constructor.
     * @see #construct(Class, Class[], Object...)
     */
    public static <T> T construct(Class<? extends T> type, Object... parameters) throws ConfigurationException
    {
        return construct(type, getParameterClasses(parameters), parameters);
    }

    /**
     * Instantiate a subclass of &lt;T&gt; with the provided parameters.
     *
     * @param type The type of the subclass.
     * @param parameterClasses The type of the parameters to the constructor
     * @param parameters The parameters to send to the constructor.
     * @param <T> The interface/class to construct a subclass for.
     * @return The constructed object.
     * @throws ConfigurationException Thrown if the constructor is invalid or if it's not possible to access the constructor.
     */
    public static <T> T construct(Class<? extends T> type, Class<?>[] parameterClasses, Object... parameters) throws ConfigurationException
    {
        try
        {
            return getConstructor(type, parameterClasses).newInstance(parameters);
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e)
        {
            throw new ConfigurationException("Unable to construct " + type, e);
        }
    }

    /**
     * Resolve a class by name that is extending the wanted class type.
     *
     * @param className The class to resolve.
     * @param wantedClass The wanted class type.
     * @param parameterClasses The expected parameters for the constructor.
     * @param <T> The wanted class
     * @return The resolved class that extends &lt;T&gt; and has the provided constructor.
     * @throws ConfigurationException
     *     Thrown if either:
     *     <ul>
     *          <li>The class is not found</li>
     *          <li>The class is not extending the wanted class</li>
     *          <li>The class does not have a constructor matching the expected parameters</li>
     *     </ul>
     */
    public static <T> Class<? extends T> resolveClassOfType(String className, Class<T> wantedClass, Class<?>... parameterClasses) throws ConfigurationException
    {
        Class<? extends T> clazz;

        try
        {
            clazz = Class.forName(className).asSubclass(wantedClass);
        }
        catch (ClassCastException e)
        {
            throw new ConfigurationException("Class " + className + " not assignable from " + wantedClass, e);
        }
        catch (ClassNotFoundException e)
        {
            throw new ConfigurationException("Class " + className + " not found in classpath", e);
        }

        getConstructor(clazz, parameterClasses);

        return clazz;
    }

    private static <T> Constructor<? extends T> getConstructor(Class<? extends T> type, Class<?>... parameterClasses) throws ConfigurationException
    {
        try
        {
            return type.getDeclaredConstructor(parameterClasses);
        }
        catch (NoSuchMethodException e)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("Class ").append(type.getName()).append(" does not have a constructor with parameter(s) of type [");
            for (Class<?> parameterClass : parameterClasses)
            {
                sb.append(parameterClass.getName()).append(',');
            }
            sb.append("]");

            throw new ConfigurationException(sb.toString(), e);
        }
    }

    private static Class<?>[] getParameterClasses(Object... parameters)
    {
        Class<?>[] parameterClasses = new Class<?>[parameters.length];

        for (int i = 0; i < parameters.length; i++)
        {
            parameterClasses[i] = parameters[i].getClass();
        }

        return parameterClasses;
    }
}
