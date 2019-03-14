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
package com.ericsson.bss.cassandra.ecchronos.core.scheduling;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.ScheduledJobException;

public class DummyJob extends ScheduledJob
{
    volatile boolean hasRun = false;

    public DummyJob(Priority priority)
    {
        super(new ConfigurationBuilder().withPriority(priority).withRunInterval(1, TimeUnit.SECONDS).build());
    }

    public boolean hasRun()
    {
        return hasRun;
    }

    @Override
    public Iterator<ScheduledTask> iterator()
    {
        return Arrays.<ScheduledTask> asList(new DummyTask()).iterator();
    }

    @Override
    public String toString()
    {
        return "DummyJob " + getPriority();
    }

    public class DummyTask extends ScheduledTask
    {
        @Override
        public boolean execute()
        {
            hasRun = true;
            return true;
        }

        @Override
        public void cleanup()
        {
            // NOOP
        }
    }

}