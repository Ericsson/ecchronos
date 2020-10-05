/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ConfigRefresher implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(ConfigRefresher.class);

    private final ConcurrentMap<Path, Runnable> knownConfigs = new ConcurrentHashMap<>();
    private final Path baseDirectory;
    private final WatchService watcher;

    private final ExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public ConfigRefresher(Path baseDirectory)
    {
        this.baseDirectory = baseDirectory.toAbsolutePath();
        WatchService watchService = null;

        try
        {
            watchService = FileSystems.getDefault().newWatchService();
            this.baseDirectory.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
        }
        catch (IOException e)
        {
            LOG.error("Unable to register watch service, configuration refresh will not work", e);
        }

        watcher = watchService;

        executor.submit(this::watchForEvents);
    }

    public void watch(Path filePath, Runnable onChange)
    {
        Path absoluteFilePath = filePath.toAbsolutePath();
        Preconditions.checkArgument(baseDirectory.equals(absoluteFilePath.getParent()),
                String.format("Config file %s is not located in %s", absoluteFilePath, baseDirectory));

        if (watcher == null)
        {
            return;
        }

        knownConfigs.put(absoluteFilePath.getFileName(), onChange);
        LOG.debug("Watching for changes in {}", absoluteFilePath);
    }

    @Override
    public void close()
    {
        try
        {
            watcher.close();
        }
        catch (IOException e)
        {
            LOG.error("Unable to close watcher");
        }

        executor.shutdownNow();

        try
        {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for config refresher shutdown", e);
        }
    }

    private void watchForEvents()
    {
        while (true)
        {
            WatchKey watchKey;

            try
            {
                watchKey = watcher.take();
                handleEvents(watchKey);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
            catch (ClosedWatchServiceException e)
            {
                LOG.debug("Watch service has been closed");
                return;
            }
            catch (Exception e)
            {
                LOG.error("Encountered unexpected exception while watching for events", e);
            }
        }
    }

    private void handleEvents(WatchKey watchKey)
    {
        try
        {
            for (WatchEvent<?> event : watchKey.pollEvents())
            {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == StandardWatchEventKinds.OVERFLOW)
                {
                    continue;
                }

                Object context = event.context();

                if (context instanceof Path)
                {
                    handleEvent((Path) context);
                }
                else
                {
                    LOG.warn("Unknown context {}", context);
                }
            }
        }
        finally
        {
            watchKey.reset();
        }
    }

    private void handleEvent(Path file)
    {
        LOG.debug("Received event for {}/{}", baseDirectory, file);

        Runnable onChange = knownConfigs.get(file);
        if (onChange != null)
        {
            try
            {
                onChange.run();
            }
            catch (Exception e)
            {
                LOG.error("Encountered unexpected exception while running callback for {}", file, e);
            }
        }
    }
}
