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

import java.security.cert.CRL;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Date;

public final class CustomCRLValidator
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomCRLValidator.class);

    private static final String CRL_STATUS = "crl.status";
    private static final String VALID = "valid";
    private static final String INVALID = "invalid";
    private static final String REVOKED = "revoked";

    public enum CRLState
    {
        INVALID,    // Dates etc failed verification
        VALID,      // Everything is valid and fine, but there is no specific non-revoked CRL
        REVOKED     // The certificate is specifically revoked by the CRL
    }

    @VisibleForTesting
    protected CRLFileManager myCRLFileManager;

    public CustomCRLValidator(final CRLConfig crlConfig)
    {
        // Create a new CRL File Manager to be used by this validator
        this.myCRLFileManager = new CRLFileManager(crlConfig);
    }

    public CRLState isCertificateCRLValid(final X509Certificate cert) // NOPMD
    {
        // Make sure previous state is cleared
        ApplicationStateHolder.getInstance().put(CRL_STATUS, null);

        // Sanity check of the provided certificate and chain
        if (cert == null)
        {
            ApplicationStateHolder.getInstance().put(CRL_STATUS, INVALID);
            return CRLState.INVALID;
        }

        // Get the current CRLs and make sure there are CRLs at all
        Collection<? extends CRL> crls = myCRLFileManager.getCurrentCRLs();
        if (crls == null || crls.isEmpty())
        {
            ApplicationStateHolder.getInstance().put(CRL_STATUS, INVALID);
            return CRLState.INVALID;
        }

        // Do the actual revoke checking
        for (CRL crl : crls)
        {
            // Check if this CRL is for our certificate's issuer
            if ((crl instanceof X509CRL x509Crl)
                    && x509Crl.getIssuerX500Principal().equals(cert.getIssuerX500Principal()))
            {
                // Certificate revoked?
                if (x509Crl.isRevoked(cert))
                {
                    LOG.warn("Certificate with serial number {} is revoked by CRL", cert.getSerialNumber());
                    ApplicationStateHolder.getInstance().put(CRL_STATUS, REVOKED);
                    return CRLState.REVOKED;
                }
                // Also, verify the CRL is actually current
                Date next = x509Crl.getNextUpdate();
                if (next != null)
                {
                    if (next.before(new Date()))
                    {
                        LOG.debug("CRL for issuer {} is expired", x509Crl.getIssuerX500Principal().getName());
                        ApplicationStateHolder.getInstance().put(CRL_STATUS, INVALID);
                        return CRLState.INVALID;
                    }
                    else
                    {
                        ApplicationStateHolder.getInstance().put(CRL_STATUS, VALID);
                        return CRLState.VALID;
                    }
                }
            }
        }
        // Gone through all CRLs, nothing was valid for this certificate
        ApplicationStateHolder.getInstance().put(CRL_STATUS, INVALID);
        return CRLState.INVALID;
    }

    public void addRefreshListener(final Runnable listener)
    {
        this.myCRLFileManager.addRefreshListener(listener);
    }

    public void resetAttempts()
    {
        this.myCRLFileManager.resetAttempts();
    }

    public int increaseAttempts()
    {
        return this.myCRLFileManager.increaseAttempts();
    }

    public int maxAttempts()
    {
        return this.myCRLFileManager.maxAttempts();
    }

    public boolean hasMoreAttempts()
    {
        return this.myCRLFileManager.hasMoreAttempts();
    }

    public boolean inStrictMode()
    {
        return this.myCRLFileManager.inStrictMode();
    }

}
