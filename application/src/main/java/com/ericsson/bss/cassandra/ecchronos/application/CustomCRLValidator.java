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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Date;

public final class CustomCRLValidator
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomCRLValidator.class);

    @VisibleForTesting
    protected CRLFileManager myCRLFileManager;

    public CustomCRLValidator(final CRLConfig crlConfig)
    {
        // Create a new CRL File Manager to be used by this validator
        this.myCRLFileManager = new CRLFileManager(crlConfig);
    }

    public void validateCertificate(final X509Certificate cert, final X509Certificate[] chain) // NOPMD
            throws CertificateException, CRLException, NoSuchAlgorithmException,
            SignatureException, InvalidKeyException, NoSuchProviderException
    {
        // Sanity check of the provided chain
        if (chain == null || chain.length < 1)
        {
            throw new CertificateException("Certificate chain must also include CA certificate");
        }
        // Get CA cert
        X509Certificate caCert = chain[0];

        // Get the latest CRLs
        Collection<? extends CRL> crls = myCRLFileManager.getCurrentCRLs();
        if (myCRLFileManager.inStrictMode())
        {
            // "Emptiness" in strict mode is not allowed
            if (crls == null || crls.isEmpty())
            {
                throw new CertificateException("In strict mode CRLs must be available for validation");
            }
            // In strict mode, verify certificate is signed by the same CA (will throw if not)
            cert.verify(caCert.getPublicKey());
        }

        // Do the actual revoke checking
        boolean validCRL = false;
        for (CRL crl : crls)
        {
            if (crl instanceof X509CRL x509Crl)
            {
                // Verify CRL is actually signed by the CA
                if (myCRLFileManager.inStrictMode())
                {
                    try
                    {
                        x509Crl.verify(caCert.getPublicKey());
                    }
                    catch (Exception e)
                    {
                        // CRL not signed by the CA. Disregard it and continue with the next
                        continue;
                    }
                }

                // Check if this CRL is for our certificate's issuer
                if (x509Crl.getIssuerX500Principal().equals(cert.getIssuerX500Principal()))
                {
                    // Certificate revoked?
                    if (x509Crl.isRevoked(cert))
                    {
                        LOG.debug("Certificate with serial number {} is revoked", cert.getSerialNumber());
                        throw new CertificateException(
                                "Certificate is revoked by CRL: " + cert.getSubjectX500Principal());
                    }
                    // Also, verify the CRL is actually current
                    if (x509Crl.getNextUpdate().before(new Date()))
                    {
                        String expCRL = ((X509CRL) crl).getIssuerX500Principal().getName();
                        LOG.debug("CRL for issuer {} is expired", expCRL);
                        throw new CertificateException("CRL is expired: " + expCRL);
                    }

                    // At least one CRL (with no revocation) passed, which is necessary for strict mode
                    validCRL = true;
                }
            }
        }

        if (myCRLFileManager.inStrictMode() && !validCRL)
        {
            throw new CertificateException("No valid CRL found for certificate issuer "
                    + cert.getIssuerX500Principal().getName());
        }

        // If this point is reached, the certificate is valid and not revoked
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
