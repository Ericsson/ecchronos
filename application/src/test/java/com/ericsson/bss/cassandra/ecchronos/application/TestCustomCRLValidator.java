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

import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import javax.security.auth.x500.X500Principal;

import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestCustomCRLValidator
{

    /**
     * Test that a valid certificate and a valid CRL is resulting in a not VALID state.
     * @throws Exception
     */
    @Test
    public void testValidateNonRevokedCertificateWithValidCRL() throws Exception
    {
        // Mock dependencies
        CRLFileManager crlFileManager = mock(CRLFileManager.class);
        X509Certificate cert = mock(X509Certificate.class);
        X509Certificate caCert = mock(X509Certificate.class);
        X509CRL x509Crl = mock(X509CRL.class);
        // Create a future date for CRL next update (24 hours from now)
        Date futureDate = new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000);
        // Setup mock behaviors
        Collection<X509CRL> crls = Collections.singleton(x509Crl);
        doReturn(crls).when(crlFileManager).getCurrentCRLs();
        X500Principal issuerPrincipal = new X500Principal("CN=Test CA");
        when(x509Crl.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(cert.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(x509Crl.isRevoked(cert)).thenReturn(false);
        when(x509Crl.getNextUpdate()).thenReturn(futureDate);
        CustomCRLValidator validator = new CustomCRLValidator(getDefaultCRLConfig());
        validator.myCRLFileManager = crlFileManager;
        // Execute validation
        assert(validator.isCertificateCRLValid(cert)).equals(CustomCRLValidator.CRLState.VALID);
    }

    /**
     * Test that a revoked certificate and a valid CRL is resulting in a REVOKED state.
     */
    @Test
    public void testValidateRevokedCertificateWithValidCRL()
    {
        // Mock dependencies
        CRLFileManager crlFileManager = mock(CRLFileManager.class);
        X509Certificate cert = mock(X509Certificate.class);
        X509CRL x509Crl = mock(X509CRL.class);
        // Create a future date for CRL next update (24 hours from now)
        Date futureDate = new Date(System.currentTimeMillis() + 24 * 60 * 60 * 1000);
        // Setup mock behaviors
        Collection<X509CRL> crls = Collections.singleton(x509Crl);
        doReturn(crls).when(crlFileManager).getCurrentCRLs();
        X500Principal issuerPrincipal = new X500Principal("CN=Test CA");
        when(x509Crl.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(cert.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(x509Crl.isRevoked(cert)).thenReturn(true);
        CustomCRLValidator validator = new CustomCRLValidator(getDefaultCRLConfig());
        validator.myCRLFileManager = crlFileManager;
        // Execute validation
        assert(validator.isCertificateCRLValid(cert)).equals(CustomCRLValidator.CRLState.REVOKED);
    }

    /**
     * Test that a valid certificate and an invalid CRL is resulting in a INVALID state.
     */
    @Test
    public void testValidateNonRevokedCertificateWithInvalidCRL()
    {
        // Mock dependencies
        CRLFileManager crlFileManager = mock(CRLFileManager.class);
        X509Certificate cert = mock(X509Certificate.class);
        X509CRL x509Crl = mock(X509CRL.class);
        // Create a future date for CRL next update (24 hours from now)
        Date pastDate = new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000);
        // Setup mock behaviors
        Collection<X509CRL> crls = Collections.singleton(x509Crl);
        doReturn(crls).when(crlFileManager).getCurrentCRLs();
        X500Principal issuerPrincipal = new X500Principal("CN=Test CA");
        when(x509Crl.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(cert.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        when(x509Crl.isRevoked(cert)).thenReturn(false);
        when(x509Crl.getNextUpdate()).thenReturn(pastDate);
        CustomCRLValidator validator = new CustomCRLValidator(getDefaultCRLConfig());
        validator.myCRLFileManager = crlFileManager;
        // Execute validation - should not throw any exception
        assert(validator.isCertificateCRLValid(cert)).equals(CustomCRLValidator.CRLState.INVALID);
    }

    private CRLConfig getDefaultCRLConfig()
    {
        return new CRLConfig();
    }

}