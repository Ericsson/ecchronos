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

import com.ericsson.bss.cassandra.ecchronos.application.config.security.CRLConfig;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class CRLFileManager
{
    private static final Logger LOG = LoggerFactory.getLogger(CRLFileManager.class);

    private static final int FIRST_ATTEMPT_COUNT = 1;

    private long lastModifiedTime = 0;
    private long lastFileSize = 0;

    private final ScheduledExecutorService myScheduler;
    private final String myPath;
    private final boolean myStrict;
    private final int myMaxAttempts;
    private int myAttempt;

    private final CertificateFactory myCertFactory;
    private final List<Runnable> myRefreshListeners = new ArrayList<>();
    private volatile Set<? extends CRL> myCRLs = new LinkedHashSet<>();

    public CRLFileManager(final CRLConfig crlConfig)
    {
        this.myPath = crlConfig.getPath();
        this.myStrict = crlConfig.getStrict();
        int maxAttempts = crlConfig.getAttempts();
        if (maxAttempts < FIRST_ATTEMPT_COUNT)
        {
            maxAttempts = FIRST_ATTEMPT_COUNT;
            LOG.warn("CRL file manager configured with invalid number of max attempts ({}), using 1 instead",
                    crlConfig.getAttempts());
        }
        this.myMaxAttempts = maxAttempts;

        this.myAttempt = FIRST_ATTEMPT_COUNT;

        this.myScheduler = Executors.newSingleThreadScheduledExecutor();

        try
        {
            this.myCertFactory = CertificateFactory.getInstance("X.509");
        }
        catch (CertificateException e)
        {
            throw new IllegalStateException("Unable to initialize CertificateFactory in CRL file manager", e);
        }
        // Schedule periodic CRL file check for updates/modifications
        myScheduler.scheduleAtFixedRate(
                this::refreshCRLs,          // Method to be called
                0,                          // Initial period (refresh first time immediately)
                crlConfig.getInterval(),    // Recurring interval
                TimeUnit.SECONDS            // Time unit
        );

        LOG.info("CRL initialized using path '{}'", this.myPath);
    }

    public Collection<? extends CRL> getCurrentCRLs()
    {
        return myCRLs;
    }

    public void addRefreshListener(final Runnable listener)
    {
        if (listener != null && !myRefreshListeners.contains(listener))
        {
            myRefreshListeners.add(listener);
        }
    }

    public List<Runnable> getRefreshListeners()
    {
        return myRefreshListeners;
    }

    public boolean inStrictMode()
    {
        return myStrict;
    }

    public int increaseAttempts()
    {
        int attempt = myAttempt;
        myAttempt++;
        return attempt;
    }

    public boolean hasMoreAttempts()
    {
        return myAttempt <= myMaxAttempts;
    }

    public void resetAttempts()
    {
        myAttempt = 1;
    }

    public int maxAttempts()
    {
        return myMaxAttempts;
    }

    @VisibleForTesting
    public void notifyRefresh()
    {
        for (Runnable listener : myRefreshListeners)
        {
            listener.run();
        }
    }

    private boolean fileModified()
    {
        File file = new File(myPath);
        return file.lastModified() != lastModifiedTime || file.length() != lastFileSize;
    }

    private void updateFileMetadata()
    {
        File file = new File(myPath);
        lastModifiedTime = file.lastModified();
        lastFileSize = file.length();
    }

    private void refreshCRLs()
    {
        if (!fileModified() && myAttempt == 1)
        {
            // No file changes and no reattempts needed, so no refresh is necessary at this time
            LOG.debug("No CRL file changes detected; skipping refresh");
            return;
        }

        try (InputStream crlFile = new BufferedInputStream(Files.newInputStream(Paths.get(myPath))))
        {
            // Read up and generate new CRL list
            long startTime = System.nanoTime();
            Collection<? extends  CRL> newCRLs = myCertFactory.generateCRLs(crlFile);
            // Create a unique Set while keeping the order (all duplicates removed)
            this.myCRLs = new LinkedHashSet<>(newCRLs);
            // Number of detected duplicates
            int dupes = newCRLs.size() - this.myCRLs.size();
            // Measure time spent on refreshing
            long endTime = System.nanoTime();
            long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            // CRL file was refreshed; notify and log the details
            LOG.info("CRL file refreshed in {} ms; read {} record(s), discarded {} duplicate(s)",
                    duration, this.myCRLs.size(), dupes);
        }
        catch (IOException | CRLException e)
        {
            myCRLs.clear();
            LOG.error("Failed to read CRL file; any previously cached CRLs have been purged");
        }
        // Make sure listeners are aware of the changes
        updateFileMetadata();
        notifyRefresh();

    }

}
