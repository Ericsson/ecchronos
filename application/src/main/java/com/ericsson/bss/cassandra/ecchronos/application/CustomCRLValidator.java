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

import java.security.cert.CRL;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Date;

/** Validates X.509 certificates against Certificate Revocation Lists. */
public final class CustomCRLValidator
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomCRLValidator.class);

    public enum CRLState
    {
        /** Certificate is invalid; Dates etc failed verification. */
        INVALID,
        /** Certificate is valid; Everything is valid and fine, but there is no specific non-revoked CRL. */
        VALID,
        /** Certificate is revoked; The certificate is specifically revoked by the CRL. */
        REVOKED
    }

    /** The CRL file manager. */
    @VisibleForTesting
    protected CRLFileManager myCRLFileManager;

    /**
     * Constructs a new CustomCRLValidator.
     * @param crlConfig the CRL config
     */
    public CustomCRLValidator(final CRLConfig crlConfig)
    {
        // Create a new CRL File Manager to be used by this validator
        this.myCRLFileManager = new CRLFileManager(crlConfig);
    }

    /**
     * Validates a certificate against the current CRLs.
     * @param cert the certificate to validate
     * @return the CRL state indicating whether the certificate is valid, revoked, or invalid
     */
    public CRLState isCertificateCRLValid(final X509Certificate cert)
    {
        if (cert == null)
        {
            return CRLState.INVALID;
        }

        Collection<? extends CRL> crls = myCRLFileManager.getCurrentCRLs();
        if (crls == null || crls.isEmpty())
        {
            return CRLState.INVALID;
        }

        for (CRL crl : crls)
        {
            CRLState result = validateAgainstCrl(cert, crl);
            if (result != null)
            {
                return result;
            }
        }

        return CRLState.INVALID;
    }

    /**
     * Validates a certificate against a single CRL entry.
     * @param cert the certificate to validate
     * @param crl the CRL to check against
     * @return the CRL state, or null if the CRL is not applicable
     */
    private CRLState validateAgainstCrl(final X509Certificate cert, final CRL crl)
    {
        if (!(crl instanceof X509CRL x509Crl))
        {
            return null;
        }
        if (!x509Crl.getIssuerX500Principal().equals(cert.getIssuerX500Principal()))
        {
            return null;
        }

        if (x509Crl.isRevoked(cert))
        {
            LOG.warn("Certificate with serial number {} is revoked by CRL", cert.getSerialNumber());
            return CRLState.REVOKED;
        }

        Date next = x509Crl.getNextUpdate(); // NOPMD Rule:ReplaceJavaUtilDate
        if (next != null && next.before(new Date())) // NOPMD Rule:ReplaceJavaUtilDate
        {
            LOG.debug("CRL for issuer {} is expired", x509Crl.getIssuerX500Principal().getName());
            return CRLState.INVALID;
        }
        return next != null ? CRLState.VALID : null;
    }

    public void addRefreshListener(final Runnable listener)
    {
        this.myCRLFileManager.addRefreshListener(listener);
    }

    /** Resets the attempt counter. */
    public void resetAttempts()
    {
        this.myCRLFileManager.resetAttempts();
    }

    /**
     * Increments the attempt counter.
     * @return the new attempt count
     */
    public int increaseAttempts()
    {
        return this.myCRLFileManager.increaseAttempts();
    }

    /**
     * Returns the maximum number of attempts.
     * @return the maximum number of attempts
     */
    public int maxAttempts()
    {
        return this.myCRLFileManager.maxAttempts();
    }

    /**
     * Returns whether it has more attempts.
     * @return true if it has more attempts
     */
    public boolean hasMoreAttempts()
    {
        return this.myCRLFileManager.hasMoreAttempts();
    }

    /**
     * Returns whether strict mode is enabled.
     * @return true if in strict mode
     */
    public boolean inStrictMode()
    {
        return this.myCRLFileManager.inStrictMode();
    }

}
