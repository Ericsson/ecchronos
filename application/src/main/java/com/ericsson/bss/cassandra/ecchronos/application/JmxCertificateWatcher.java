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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls a set of JMX/Jolokia client-certificate files (certificate, private key, CA) on a fixed interval and notifies
 * registered listeners when any of them changes on disk.
 *
 * <p>This is the proactive trigger for issue #1961: on an on-disk certificate rotation the listener invalidates the
 * managed JMX connectors so the next lazy {@code getJmxConnector(...)} rebuilds them with the new certificate (the
 * managed {@code SSLContext} from #1970), pre-empting the single post-rotation handshake failure that the reactive
 * fix in #1960 would otherwise absorb. Modeled on {@code CRLFileManager}.
 *
 * <p>Change detection compares {@code lastModified} and file length, matching {@code CRLFileManager}; it does not
 * parse the certificates (the connector rebuild re-reads them). The watcher only schedules polling when it is given
 * at least one file to watch, so callers gate construction on JMX TLS being enabled and certificate-configured.
 */
public final class JmxCertificateWatcher implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(JmxCertificateWatcher.class);

    private final List<String> myPaths;
    private final List<Runnable> myRefreshListeners = new ArrayList<>();
    private final Map<String, long[]> myFileMetadata = new HashMap<>();
    private final ScheduledExecutorService myScheduler;

    /**
     * Constructs and starts a watcher over the given certificate files.
     *
     * @param paths              the certificate/key/CA file paths to watch; must contain at least one non-null path.
     * @param intervalInSeconds  the polling interval in seconds; values below 1 are clamped to 1.
     */
    public JmxCertificateWatcher(final List<String> paths, final int intervalInSeconds)
    {
        myPaths = new ArrayList<>();
        for (String path : paths)
        {
            if (path != null && !path.isEmpty())
            {
                myPaths.add(path);
            }
        }
        // Seed the baseline so the first poll does not spuriously fire for pre-existing files.
        for (String path : myPaths)
        {
            myFileMetadata.put(path, currentMetadata(path));
        }

        int interval = Math.max(1, intervalInSeconds);
        myScheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "JmxCertificateWatcher");
            thread.setDaemon(true);
            return thread;
        });
        myScheduler.scheduleAtFixedRate(this::checkForChanges, interval, interval, TimeUnit.SECONDS);
        LOG.info("JMX certificate watcher started for {} file(s), polling every {}s", myPaths.size(), interval);
    }

    /**
     * Registers a listener invoked when any watched file changes on disk.
     *
     * @param listener the listener to register; ignored if {@code null} or already registered.
     */
    public void addRefreshListener(final Runnable listener)
    {
        if (listener != null && !myRefreshListeners.contains(listener))
        {
            myRefreshListeners.add(listener);
        }
    }

    @VisibleForTesting
    List<Runnable> getRefreshListeners()
    {
        return myRefreshListeners;
    }

    @VisibleForTesting
    void checkForChanges()
    {
        boolean changed = false;
        for (String path : myPaths)
        {
            long[] current = currentMetadata(path);
            long[] previous = myFileMetadata.get(path);
            if (previous == null || current[0] != previous[0] || current[1] != previous[1])
            {
                myFileMetadata.put(path, current);
                changed = true;
            }
        }
        if (changed)
        {
            LOG.info("Detected a change in the JMX client-certificate files; invalidating JMX connectors");
            notifyRefresh();
        }
    }

    private void notifyRefresh()
    {
        for (Runnable listener : myRefreshListeners)
        {
            try
            {
                listener.run();
            }
            catch (RuntimeException e)
            {
                LOG.warn("JMX certificate refresh listener failed", e);
            }
        }
    }

    private static long[] currentMetadata(final String path)
    {
        File file = new File(path);
        return new long[]
                {
                        file.lastModified(),
                        file.length()
                };
    }

    @Override
    public void close()
    {
        myScheduler.shutdownNow();
    }
}
