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

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestJmxCertificateWatcher
{
    @Rule
    public TemporaryFolder myTemporaryFolder = new TemporaryFolder();

    @Test
    public void testFiresListenerWhenFileChanges() throws Exception
    {
        File cert = myTemporaryFolder.newFile("client.crt");
        Files.writeString(cert.toPath(), "initial");

        AtomicInteger fired = new AtomicInteger();
        // A very large interval so only the explicit checkForChanges() drives the test (no scheduler races).
        JmxCertificateWatcher watcher = new JmxCertificateWatcher(List.of(cert.getAbsolutePath()), 3600);
        try
        {
            watcher.addRefreshListener(fired::incrementAndGet);

            // No change yet -> no fire.
            watcher.checkForChanges();
            assertThat(fired.get()).isZero();

            // Rotate the cert on disk (change content + bump mtime to be robust on coarse filesystems).
            Files.writeString(cert.toPath(), "rotated-with-different-length");
            cert.setLastModified(System.currentTimeMillis() + 2000);

            watcher.checkForChanges();
            assertThat(fired.get()).isEqualTo(1);

            // Stable again -> no further fire.
            watcher.checkForChanges();
            assertThat(fired.get()).isEqualTo(1);
        }
        finally
        {
            watcher.close();
        }
    }

    @Test
    public void testFiresWhenAnyOfSeveralFilesChanges() throws Exception
    {
        File cert = myTemporaryFolder.newFile("client.crt");
        File key = myTemporaryFolder.newFile("client.key");
        File ca = myTemporaryFolder.newFile("ca.crt");
        Files.writeString(cert.toPath(), "c");
        Files.writeString(key.toPath(), "k");
        Files.writeString(ca.toPath(), "a");

        AtomicInteger fired = new AtomicInteger();
        JmxCertificateWatcher watcher = new JmxCertificateWatcher(
                List.of(cert.getAbsolutePath(), key.getAbsolutePath(), ca.getAbsolutePath()), 3600);
        try
        {
            watcher.addRefreshListener(fired::incrementAndGet);

            // Only the CA changes.
            Files.writeString(ca.toPath(), "a-rotated");
            ca.setLastModified(System.currentTimeMillis() + 2000);

            watcher.checkForChanges();
            assertThat(fired.get()).isEqualTo(1);
        }
        finally
        {
            watcher.close();
        }
    }

    @Test
    public void testNullPathsAreIgnored() throws Exception
    {
        File cert = myTemporaryFolder.newFile("client.crt");
        Files.writeString(cert.toPath(), "c");

        AtomicInteger fired = new AtomicInteger();
        // key/ca absent (null) - only the cert is watched; must not throw and must still detect the cert change.
        JmxCertificateWatcher watcher = new JmxCertificateWatcher(
                java.util.Arrays.asList(cert.getAbsolutePath(), null, null), 3600);
        try
        {
            watcher.addRefreshListener(fired::incrementAndGet);
            Files.writeString(cert.toPath(), "c-rotated");
            cert.setLastModified(System.currentTimeMillis() + 2000);
            watcher.checkForChanges();
            assertThat(fired.get()).isEqualTo(1);
        }
        finally
        {
            watcher.close();
        }
    }

    @Test
    public void testListenerExceptionDoesNotStopOtherListeners() throws Exception
    {
        File cert = myTemporaryFolder.newFile("client.crt");
        Files.writeString(cert.toPath(), "c");

        AtomicInteger second = new AtomicInteger();
        JmxCertificateWatcher watcher = new JmxCertificateWatcher(List.of(cert.getAbsolutePath()), 3600);
        try
        {
            watcher.addRefreshListener(() ->
            {
                throw new IllegalStateException("boom");
            });
            watcher.addRefreshListener(second::incrementAndGet);

            Files.writeString(cert.toPath(), "c-rotated");
            cert.setLastModified(System.currentTimeMillis() + 2000);
            watcher.checkForChanges();

            assertThat(second.get()).isEqualTo(1);
        }
        finally
        {
            watcher.close();
        }
    }
}
