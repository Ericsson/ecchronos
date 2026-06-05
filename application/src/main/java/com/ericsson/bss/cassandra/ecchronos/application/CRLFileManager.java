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

import com.ericsson.bss.cassandra.ecchronos.core.state.ApplicationStateHolder;
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
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Manages Certificate Revocation List files and their periodic refresh. */
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

    /**
     * Constructs a new CRLFileManager.
     * @param crlConfig the CRL config
     */
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

    /**
     * Returns the current CRLs.
     * @return the current CRLs
     */
    public Collection<? extends CRL> getCurrentCRLs()
    {
        return myCRLs;
    }

    /**
     * Adds refresh listener.
     * @param listener the event listener to register
     */
    public void addRefreshListener(final Runnable listener)
    {
        if (listener != null && !myRefreshListeners.contains(listener))
        {
            myRefreshListeners.add(listener);
        }
    }

    /**
     * Returns the refresh listeners.
     * @return the refresh listeners
     */
    public List<Runnable> getRefreshListeners()
    {
        return myRefreshListeners;
    }

    /**
     * Returns whether strict mode is enabled.
     * @return true if in strict mode
     */
    public boolean inStrictMode()
    {
        return myStrict;
    }

    /**
     * Increments the attempt counter.
     * @return the new attempt count
     */
    public int increaseAttempts()
    {
        int attempt = myAttempt;
        myAttempt++;
        return attempt;
    }

    /**
     * Returns whether it has more attempts.
     * @return true if it has more attempts
     */
    public boolean hasMoreAttempts()
    {
        return myAttempt <= myMaxAttempts;
    }

    /** Resets the attempt counter. */
    public void resetAttempts()
    {
        myAttempt = 1;
    }

    /**
     * Returns the maximum number of attempts.
     * @return the maximum number of attempts
     */
    public int maxAttempts()
    {
        return myMaxAttempts;
    }

    /**
     * Returns the attempt.
     * @return the attempt
     */
    public int getAttempt()
    {
        return myAttempt;
    }

    /** Notifies listeners that a refresh has occurred. */
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
            ApplicationStateHolder.getInstance().put("crl.last_refresh", startTime);

            // Update internal status with the latest CRLs read
            List<Map<String, Object>> result = new ArrayList<>();
            for (CRL crl : this.myCRLs)
            {
                if (crl instanceof X509CRL x509Crl)
                {
                    Map<String, Object> temp = new HashMap<>();
                    temp.put("issuer", x509Crl.getIssuerX500Principal().getName());
                    temp.put("next_update", x509Crl.getNextUpdate());
                    temp.put("type", x509Crl.getType());
                    Set<? extends X509CRLEntry> revokedCerts = x509Crl.getRevokedCertificates();
                    List<Map<String, Object>> revokedList = new ArrayList<>();
                    if (revokedCerts != null)
                    {
                        for (X509CRLEntry cert : revokedCerts)
                        {
                             Map<String, Object> revoked = new HashMap<>();
                             revoked.put("serial_number", cert.getSerialNumber());
                             revoked.put("revocation_date", cert.getRevocationDate());
                             revoked.put("critical_extensions", cert.getCriticalExtensionOIDs());
                             revokedList.add(revoked);
                        }
                    }
                    temp.put("revoked_certificates", revokedList);
                    result.add(temp);
                }
            }
            ApplicationStateHolder.getInstance().put("crl.entries", result);
        }
        catch (IOException | CRLException e)
        {
            myCRLs.clear();
            LOG.error("Failed to read CRL file; any previously cached CRLs have been purged");
            ApplicationStateHolder.getInstance().put("crl.last_refresh", -1);
        }
        // Make sure listeners are aware of the changes
        updateFileMetadata();
        notifyRefresh();
    }

}
