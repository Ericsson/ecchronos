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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CRLException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class TestCustomX509TrustManager
{

    /**
     * Tests the checkServerTrusted method of CustomX509TrustManager.
     * This test verifies that the method correctly delegates to the underlying trust manager,
     * performs CRL validation, and stores the last validated chain and auth type.
     * @throws CertificateException if certificate validation fails
     */
    @Test
    public void testCheckServerTrustedValidatesAndStoresChain() throws CertificateException
    {
        // Mock dependencies
        X509TrustManager mockDelegate = mock(X509TrustManager.class);
        CustomCRLValidator mockValidator = mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(mockDelegate, mockValidator);
        X509Certificate[] chain = new X509Certificate[1];
        chain[0] = mock(X509Certificate.class);
        String authType = "RSA";
        // Set up expectations
        doNothing().when(mockDelegate).checkServerTrusted(chain, authType);
        // Execute method under test
        trustManager.checkServerTrusted(chain, authType);
        // Verify interactions
        verify(mockDelegate).checkServerTrusted(chain, authType);
        verify(mockValidator).isCertificateCRLValid(chain[0]);
        // Verify that the last chain and auth type were stored
        assertArrayEquals(trustManager.myLastServerChain, chain);
        assertEquals(authType, trustManager.myLastServerAuthType);
    }

    /**
     * Tests a system exit will be performed when all failing attempts have been made.
     * @throws CertificateException
     */
    @Test
    public void testOnRefreshWithCertificateException() throws CertificateException
    {
        // Create mocks
        X509TrustManager mockDelegate = mock(X509TrustManager.class);
        CustomCRLValidator mockCRLValidator = mock(CustomCRLValidator.class);
        // Create spy of the class
        CustomX509TrustManager trustManager = spy(new CustomX509TrustManager(mockDelegate, mockCRLValidator));
        // Configure mock behavior to throw exception
        doThrow(new CertificateException()).when(trustManager).revalidateServerTrust();
        when(mockCRLValidator.inStrictMode()).thenReturn(true);
        when(mockCRLValidator.increaseAttempts()).thenReturn(3);
        when(mockCRLValidator.maxAttempts()).thenReturn(3);
        when(mockCRLValidator.hasMoreAttempts()).thenReturn(false);
        doNothing().when(trustManager).systemExit();
        // Execute the method
        trustManager.onRefresh();
        // Verify method calls
        verify(trustManager).revalidateServerTrust();
        verify(mockCRLValidator).inStrictMode();
        verify(mockCRLValidator).increaseAttempts();
        verify(mockCRLValidator).maxAttempts();
        verify(trustManager).systemExit();
    }

    /**
     * Tests checkClientTrusted throws when certificate is REVOKED.
     */
    @Test
    public void testCheckClientTrustedCRLValidationFails() throws Exception
    {
        X509TrustManager delegateMock = Mockito.mock(X509TrustManager.class);
        CustomCRLValidator validatorMock = Mockito.mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(delegateMock, validatorMock);
        X509Certificate[] dummyChain = new X509Certificate[1];
        dummyChain[0] = Mockito.mock(X509Certificate.class);
        doReturn(CustomCRLValidator.CRLState.REVOKED).when(validatorMock).isCertificateCRLValid(Mockito.any(X509Certificate.class));
        assertThrows(CertificateException.class, () -> trustManager.checkClientTrusted(dummyChain, "RSA"));
    }

    /**
     * Tests the behavior of checkClientTrusted when the delegate throws a CertificateException.
     */
    @Test
    public void testCheckClientTrustedDelegateThrowsCertificateException() throws CertificateException
    {
        X509TrustManager delegateMock = Mockito.mock(X509TrustManager.class);
        CustomCRLValidator validatorMock = Mockito.mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(delegateMock, validatorMock);
        X509Certificate[] dummyChain = new X509Certificate[1];
        doThrow(new CertificateException("Delegate exception")).when(delegateMock).checkClientTrusted(dummyChain, "RSA");
        assertThrows(CertificateException.class, () -> trustManager.checkClientTrusted(dummyChain, "RSA"));
    }

    /**
     * Tests the behavior of checkClientTrusted when provided with null certificate chain.
     */
    @Test
    public void testCheckClientTrustedNullCertificateChain()
    {
        X509TrustManager delegateMock = Mockito.mock(X509TrustManager.class);
        CustomCRLValidator validatorMock = Mockito.mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(delegateMock, validatorMock);
        assertThrows(NullPointerException.class, () -> trustManager.checkClientTrusted(null, "RSA"));
    }

    /**
     * Test case for CRL validator throwing CertificateException when certificate is REVOKED.
     */
    @Test
    public void testCheckServerTrustedCRLValidatorThrowsException() throws Exception
    {
        X509TrustManager delegateMock = Mockito.mock(X509TrustManager.class);
        CustomCRLValidator validatorMock = Mockito.mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(delegateMock, validatorMock);
        X509Certificate[] chain = new X509Certificate[1];
        chain[0] = Mockito.mock(X509Certificate.class);
        String authType = "RSA";
        doReturn(CustomCRLValidator.CRLState.REVOKED).when(validatorMock).isCertificateCRLValid(Mockito.any(X509Certificate.class));
        assertThrows(CertificateException.class, () -> trustManager.checkClientTrusted(chain, authType));
    }

    /**
     * Negative test case for getAcceptedIssuers when the delegate returns an empty array.
     */
    @Test
    public void testGetAcceptedIssuersDelegateReturnsEmptyArray()
    {
        X509TrustManager mockDelegate = Mockito.mock(X509TrustManager.class);
        when(mockDelegate.getAcceptedIssuers()).thenReturn(new X509Certificate[0]);
        CustomCRLValidator mockValidator = Mockito.mock(CustomCRLValidator.class);
        CustomX509TrustManager trustManager = new CustomX509TrustManager(mockDelegate, mockValidator);
        X509Certificate[] result = trustManager.getAcceptedIssuers();
        assertNotNull("getAcceptedIssuers should not return null", result);
        assertEquals("getAcceptedIssuers should return an empty array when delegate returns an empty array",0, result.length);
    }

    /**
     * Test case for revalidateServerTrust when CRL validation fails (exception thrown)
     * @throws Exception if an unexpected exception occurs
     */
    @Test
    public void testRevalidateServerTrustCRLValidationFails() throws Exception
    {
        X509TrustManager mockDelegate = mock(X509TrustManager.class);
        CustomCRLValidator mockValidator = mock(CustomCRLValidator.class);
        CustomX509TrustManager customTrustManager = new CustomX509TrustManager(mockDelegate, mockValidator);
        X509Certificate[] dummyChain = new X509Certificate[]{mock(X509Certificate.class)};
        String dummyAuthType = "RSA";
        customTrustManager.checkServerTrusted(dummyChain, dummyAuthType);
        doReturn(CustomCRLValidator.CRLState.REVOKED).when(mockValidator).isCertificateCRLValid(any());
        assertThrows(CertificateException.class, customTrustManager::revalidateServerTrust);
    }

    /**
     * Test case for revalidateServerTrust() method when both myLastServerChain and myLastServerAuthType are null.
     * @throws CertificateException if an unexpected certificate exception occurs
     */
    @Test
    public void testRevalidateServerTrustWhenLastServerChainAndAuthTypeAreNull() throws CertificateException
    {
        X509TrustManager mockDelegate = mock(X509TrustManager.class);
        CustomCRLValidator crlValidator = mock(CustomCRLValidator.class);
        CustomX509TrustManager customTrustManager = new CustomX509TrustManager(mockDelegate, crlValidator);
        customTrustManager.revalidateServerTrust();
        verify(mockDelegate, never()).checkServerTrusted(any(X509Certificate[].class), anyString());
    }

}