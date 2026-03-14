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
package com.ericsson.bss.cassandra.ecchronos.application.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestConfigRefresher
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testGetFileContent() throws Exception
    {
        File file = temporaryFolder.newFile();

        try (ConfigRefresher configRefresher = new ConfigRefresher(temporaryFolder.getRoot().toPath()))
        {
            AtomicReference<String> reference = new AtomicReference<>(readFileContent(file));

            configRefresher.watch(file.toPath(), () -> reference.set(readFileContent(file)));

            writeToFile(file, "some content");
            await()
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> "some content".equals(reference.get()));

            writeToFile(file, "some new content");
            await()
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> "some new content".equals(reference.get()));
        }
    }

    @Test
    public void testRunnableThrowsAtFirstUpdate() throws Exception
    {
        File file = temporaryFolder.newFile();

        AtomicBoolean shouldThrow = new AtomicBoolean(true);

        try (ConfigRefresher configRefresher = new ConfigRefresher(temporaryFolder.getRoot().toPath()))
        {
            AtomicReference<String> reference = new AtomicReference<>(readFileContent(file));

            configRefresher.watch(file.toPath(), () -> {
                if (shouldThrow.get())
                {
                    throw new NullPointerException();
                }

                reference.set(readFileContent(file));
            });

            writeToFile(file, "some content");

            Thread.sleep(100);

            assertThat(reference).hasValue("");

            shouldThrow.set(false);

            writeToFile(file, "some new content");
            await()
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> "some new content".equals(reference.get()));
        }
    }

    private void writeToFile(File file, String content) throws IOException
    {
        try (FileWriter fileWriter = new FileWriter(file))
        {
            fileWriter.write(content);
        }
    }

    private String readFileContent(File file)
    {
        try (FileReader fileReader = new FileReader(file);
             BufferedReader bufferedReader = new BufferedReader(fileReader))
        {
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null)
            {
                result.append(line);
            }
            return result.toString();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return null;
    }
}
