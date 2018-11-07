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
package com.ericsson.bss.cassandra.ecchronos.core;

/**
 * Interface used to be able to mock classes that uses timing.
 *
 */
public interface Clock
{
    Clock DEFAULT = new StandardClock();

    /**
     * Get the current time in milliseconds.
     *
     * @return The current time in milliseconds.
     */
    long getTime();

    /**
     * The standard clock used which returns the current system time.
     */
    class StandardClock implements Clock
    {

        @Override
        public long getTime()
        {
            return System.currentTimeMillis();
        }
    }
}
