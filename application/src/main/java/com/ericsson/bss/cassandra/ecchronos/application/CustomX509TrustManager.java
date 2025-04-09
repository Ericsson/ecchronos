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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.X509TrustManager;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

public final class CustomX509TrustManager implements X509TrustManager
{

    private static final Logger LOG = LoggerFactory.getLogger(CustomX509TrustManager.class);

    private final X509TrustManager myDelegate;
    private final CustomCRLValidator myCRLValidator;

    @VisibleForTesting
    protected volatile X509Certificate[] myLastServerChain;

    @VisibleForTesting
    protected volatile String myLastServerAuthType;

    @VisibleForTesting
    protected volatile X509Certificate[] myLastClientChain;

    @VisibleForTesting
    protected volatile String myLastClientAuthType;

    private final ReentrantLock myValidationLock = new ReentrantLock();

    public CustomX509TrustManager(final X509TrustManager delegate, final CustomCRLValidator validator)
    {
        this.myDelegate = delegate;
        this.myCRLValidator = validator;
        this.myCRLValidator.addRefreshListener(this::onRefresh);
    }

    @Override
    public void checkServerTrusted(final X509Certificate[] chain, final String authType)
            throws CertificateException
    {
        myValidationLock.lock();
        try
        {
            // Do the override stuff
            myDelegate.checkServerTrusted(chain, authType);
            // Now, do our custom checks
            for (X509Certificate cert : chain)
            {
                myCRLValidator.validateCertificate(cert, getAcceptedIssuers());
            }
            // Store the last validated chain and authType for the server checks
            myLastServerChain = Arrays.copyOf(chain, chain.length);
            myLastServerAuthType = authType;
        }
        catch (CRLException | NoSuchAlgorithmException | SignatureException
               | InvalidKeyException | NoSuchProviderException e)
        {
            throw new CertificateException("CRL validation failed for server trusted", e);
        }
        finally
        {
            myValidationLock.unlock();
        }
    }

    @Override
    public void checkClientTrusted(final X509Certificate[] chain, final String authType)
            throws CertificateException
    {
        myValidationLock.lock();
        try
        {
            // Do the regular client stuff
            myDelegate.checkClientTrusted(chain, authType);
            // Now, do our custom checks
            for (X509Certificate cert : chain)
            {
                myCRLValidator.validateCertificate(cert, getAcceptedIssuers());
            }
            // Store the last validated chain and authType for the client checks
            myLastClientChain = Arrays.copyOf(chain, chain.length);
            myLastClientAuthType = authType;
        }
        catch (CRLException | NoSuchAlgorithmException | SignatureException
               | InvalidKeyException | NoSuchProviderException e)
        {
            throw new CertificateException("CRL validation failed for client trusted", e);
        }
        finally
        {
            myValidationLock.unlock();
        }
    }

    public void revalidateServerTrust() throws CertificateException
    {
        myValidationLock.lock();
        try
        {
            if (myLastServerChain != null && myLastServerAuthType != null)
            {
                checkServerTrusted(myLastServerChain, myLastServerAuthType);
            }
        }
        finally
        {
            myValidationLock.unlock();
        }
    }

    public void revalidateClientTrust() throws CertificateException
    {
        myValidationLock.lock();
        try
        {
            if (myLastClientChain != null && myLastClientAuthType != null)
            {
                checkClientTrusted(myLastClientChain, myLastClientAuthType);
            }
        }
        finally
        {
            myValidationLock.unlock();
        }
    }

    public void onRefresh()
    {
        try
        {
            revalidateServerTrust();
            revalidateClientTrust();
            myCRLValidator.resetAttempts();
            LOG.info("Certificates are not revoked by current CRL (any previous failed attempts was reset)");
        }
        catch (CertificateException e)
        {
            if (myCRLValidator.inStrictMode())
            {
                // Strict mode: Log it, check for attempts made and eventually shut down if all attempts are consumed
                LOG.warn("Certificates are revoked by current CRL (strict mode, attempt {} of {} made)",
                        myCRLValidator.increaseAttempts(),
                        myCRLValidator.maxAttempts());
                // If the last attempt was made, do a graceful shutdown
                if (!myCRLValidator.hasMoreAttempts())
                {
                    LOG.error("A last failed CRL attempt was made and ecChronos will be shut down now!");
                    systemExit();
                }
            }
            else
            {
                // Non-strict mode: Log a warning, but all connections will be kept alive. CRL checking will only
                // be done when setting up new connections.
                LOG.warn("No valid certificate found during CRL refresh (non-strict mode)");
            }
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers()
    {
        return myDelegate.getAcceptedIssuers();
    }

    @VisibleForTesting
    protected void systemExit()
    {
        System.exit(1);
    }

}
