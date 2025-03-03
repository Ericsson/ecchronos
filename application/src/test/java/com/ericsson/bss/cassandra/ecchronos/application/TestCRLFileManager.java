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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.ericsson.bss.cassandra.ecchronos.application.config.security.CRLConfig;

import java.nio.file.Paths;
import java.security.cert.CRL;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.ericsson.bss.cassandra.ecchronos.application.utils.CertUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;

import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCRLFileManager
{
    private static final CertUtils certUtils = new CertUtils();

    private static final String KEYSTORE_PASSWORD = "ecctest";
    private static final String STORE_TYPE_JKS = "JKS";
    private static final String INVALID_PATH = "invalid/path/to/crl";

    private static String clientCaCert;
    private static String clientCaCertKey;
    private static String clientCert;
    private static String clientCertKey;

    private static String clientKeyStore;
    private static String clientTrustStore;

    private static String tempDir;
    private static String crlRevoked;
    private static String crlNotRevoked;
    private static String crlNotRevokedDuplicate;
    private static String crlCorrupted;

    private static TestAppender testLogger = new TestAppender();

    public static class TestAppender extends ListAppender<ILoggingEvent>
    {
        public void reset() {
            this.list.clear();
        }
    }

    @BeforeClass
    public static void initOnce() throws Exception
    {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        tempDir = temporaryFolder.getRoot().getPath();
        setupCertsAndCRLs();
        testLogger.start();

        Logger logger = (Logger) LoggerFactory.getLogger(CRLFileManager.class);
        logger.addAppender(testLogger);
    }

    @AfterClass
    public static void testsDone()
    {
        testLogger.stop();
    }

    /**
     * Test that two unique listeners are added as expected.
     */
    @Test
    public void testGetRefreshListeners()
    {
        CRLFileManager manager = new CRLFileManager(getDefaultCRLConfig());
        Runnable mockListener1 = () -> {};
        Runnable mockListener2 = () -> {};
        manager.addRefreshListener(mockListener1);
        manager.addRefreshListener(mockListener2);
        assertEquals(2, manager.getRefreshListeners().size());
    }

    /**
     * Test that adding the same listener multiple times will only result in it
     * being added just once; e.g. no duplicate listeners allowed.
     */
    @Test
    public void testAddRefreshListenerWithDuplicateListeners()
    {
        CRLConfig crlConfig = getCustomCRLConfig(true, INVALID_PATH, true, 1, 1);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        Runnable listener = () -> {};
        manager.addRefreshListener(listener);
        manager.addRefreshListener(listener);
        final int[] count = {0};
        Runnable countingListener = () -> count[0]++;
        manager.addRefreshListener(countingListener);
        manager.addRefreshListener(countingListener);
        manager.notifyRefresh();
        assertEquals(1, count[0]);
    }

    /**
     * Test case for where the getCurrentCRLs actually returns a valid CRL.
     */
    @Ignore //temporary ignore due to blinking
    @Test
    public void testRefreshWithValidCRLFile() throws Exception
    {
        CRLConfig crlConfig = getCustomCRLConfig(true, crlNotRevoked, true, 5, 1);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        await().atLeast(5, TimeUnit.SECONDS);
        Collection<? extends CRL> crls = manager.getCurrentCRLs();
        await().atMost(5, TimeUnit.SECONDS).until(() -> !manager.getCurrentCRLs().isEmpty());
        assertNotNull(crls);
        assertFalse(crls.isEmpty());
    }

    /**
     * Test case for where the getCurrentCRLs actually returns a valid CRL (but the file has a duplicate entry).
     */
    @Test
    public void testRefreshWithValidCRLFileWithDuplicates() throws Exception
    {
        CRLConfig crlConfig = getCustomCRLConfig(true, crlNotRevokedDuplicate, true, 5, 1);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        await().atMost(10, TimeUnit.SECONDS).until(() -> !manager.getCurrentCRLs().isEmpty());
        Collection<? extends CRL> crls = manager.getCurrentCRLs();
        assertFalse(crls.isEmpty());
        assertEquals(1, crls.size());
    }

    /**
     * Test case for a non-valid CRL file that no CRLs are returned.
     */
    @Test
    public void testRefreshWithInvalidFile()
    {
        // Test with invalid CRL file (e.g. File Not Found)
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setEnabled(true);
        crlConfig.setPath(INVALID_PATH);
        // Create CRL file manager to test (manager will refresh immediately)
        CRLFileManager manager = new CRLFileManager(crlConfig);
        Collection<? extends CRL> crls = manager.getCurrentCRLs();
        // Assert that CRLs are empty or null when file is invalid
        Assert.assertTrue(crls == null || crls.isEmpty());
    }

    /**
     * Negative test case for getCurrentCRLs when the CRL file is corrupted.
     */
    @Test
    public void testGetCurrentCRLsWithCorruptedCRLFile()
    {
        CRLFileManager manager = new CRLFileManager(getCustomCRLConfig(true, crlCorrupted, true, 3, 1));
        Collection<? extends CRL> crls = manager.getCurrentCRLs();
        assertEquals("Expected empty list for corrupted CRL file", 0, crls.size());
    }

    /**
     * Tests the hasMoreAttempts method when the number of attempts exceeds the maximum.
     */
    @Test
    public void testHasMoreAttemptsExceedsMaxAttempts()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        for (int i = 1; i < 6; i++) {
            manager.increaseAttempts();
        }
        assertFalse(manager.hasMoreAttempts());
    }

    /**
     * Tests the hasMoreAttempts method when the maximum attempts is set to a negative value.
     */
    @Test
    public void testHasMoreAttemptsWhenMaxAttemptsIsNegative()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        crlConfig.setAttempts(-1);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        assertTrue(manager.hasMoreAttempts());
    }

    /**
     * Tests the hasMoreAttempts method when the maximum attempts is set to zero.
     */
    @Test
    public void testHasMoreAttemptsMaxAttemptsIsZero()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        crlConfig.setAttempts(0);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        assertTrue(manager.hasMoreAttempts());
    }

    /**
     * Tests that hasMoreAttempts() returns true when the current attempt is less than or equal to the maximum attempts.
     */
    @Test
    public void test_hasMoreAttempts_whenAttemptsRemaining()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        crlConfig.setAttempts(3);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        assertTrue(manager.hasMoreAttempts());
        manager.increaseAttempts();
        assertTrue(manager.hasMoreAttempts());
        manager.increaseAttempts();
        assertTrue(manager.hasMoreAttempts());
        manager.increaseAttempts();
        assertFalse(manager.hasMoreAttempts());
    }

    /**
     * Verifies that the method correctly returns the value of myStrict.
     */
    @Test
    public void testInStrictModeReturnsCorrectValue()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        crlConfig.setStrict(true);
        CRLFileManager manager = new CRLFileManager(crlConfig);
        assertTrue(manager.inStrictMode());
        crlConfig.setStrict(false);
        assertTrue(manager.inStrictMode());
    }

    /**
     * Verifies that the method correctly increments and returns the attempt count.
     */
    @Test
    public void testIncreaseAttemptsIncrementsAndReturnsAttemptCount()
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setPath(crlNotRevoked);
        CRLFileManager crlFileManager = new CRLFileManager(crlConfig);
        int firstAttempt = crlFileManager.increaseAttempts();
        int secondAttempt = crlFileManager.increaseAttempts();
        assertEquals("First attempt should return 1", 1, firstAttempt);
        assertEquals("Second attempt should return 2", 2, secondAttempt);
    }

    /**
     * Negative test case for maxAttempts method.
     */
    @Test
    public void testMaxIntegerValueForMaxAttempts()
    {
        CRLConfig config = getDefaultCRLConfig();
        config.setPath(crlNotRevoked);
        config.setAttempts(Integer.MAX_VALUE);
        CRLFileManager manager = new CRLFileManager(config);
        assertEquals("maxAttempts should return Integer.MAX_VALUE when CRLConfig is set to maximum integer value", Integer.MAX_VALUE, manager.maxAttempts());
    }

    /**
     * Negative test case for maxAttempts method.
     */
    @Test
    public void testNegativeMaxAttemptsValue()
    {
        CRLConfig config = getDefaultCRLConfig();
        config.setPath(crlNotRevoked);
        config.setAttempts(-123);
        CRLFileManager manager = new CRLFileManager(config);
        assertEquals("maxAttempts should return 1, even if a negative value is set in the config",1, manager.maxAttempts());
    }

    /**
     * Test case for resetAttempts method.
     */
    @Test
    public void testResetAttemptsMultipleCalls()
    {
        CRLConfig config = getDefaultCRLConfig();
        config.setPath(crlNotRevoked);
        CRLFileManager manager = new CRLFileManager(config);
        for (int i = 0; i < 5; i++)
        {
            manager.resetAttempts();
        }
        assertEquals(1, manager.increaseAttempts());
    }

    /**
     * Test case for resetAttempts method.
     */
    @Test
    public void testResetsAttemptCounterToOne()
    {
        CRLConfig config = getDefaultCRLConfig();
        config.setPath(crlNotRevoked);
        CRLFileManager manager = new CRLFileManager(config);
        manager.increaseAttempts();
        int afterIncreases = manager.increaseAttempts();
        assertEquals(2, afterIncreases);
        assertTrue(manager.hasMoreAttempts());
        manager.resetAttempts();
        assertEquals(1, manager.increaseAttempts());
    }

    private CRLConfig getDefaultCRLConfig()
    {
        return new CRLConfig();
    }

    private CRLConfig getCustomCRLConfig(final boolean enabled,
                                         final String path,
                                         final boolean strict,
                                         final int attempts,
                                         final int interval)
    {
        CRLConfig crlConfig = getDefaultCRLConfig();
        crlConfig.setEnabled(enabled);
        crlConfig.setPath(path);
        crlConfig.setStrict(strict);
        crlConfig.setAttempts(attempts);
        crlConfig.setInterval(interval);
        return crlConfig;
    }

    private static void setupCertsAndCRLs() throws Exception
    {
        // Get dates to use for certificates
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plus(java.time.Duration.ofHours(1)));
        // Create certificates
        clientCaCert = Paths.get(tempDir, "clientca.crt").toString();
        clientCaCertKey = Paths.get(tempDir, "clientca.key").toString();
        certUtils.createSelfSignedCACertificate("clientCA", notBefore, notAfter, "RSA", clientCaCert, clientCaCertKey);
        clientCert = Paths.get(tempDir, "client.crt").toString();
        clientCertKey = Paths.get(tempDir, "client.key").toString();
        certUtils.createCertificate("client", notBefore, notAfter, clientCaCert, clientCaCertKey, clientCert, clientCertKey);
        // Create key-/truststores
        clientKeyStore = Paths.get(tempDir, "client.keystore").toString();
        certUtils.createKeyStore(clientCert, clientCertKey, STORE_TYPE_JKS, KEYSTORE_PASSWORD, clientKeyStore);
        clientTrustStore = Paths.get(tempDir, "client.truststore").toString();
        certUtils.createTrustStore(clientCaCert, STORE_TYPE_JKS, KEYSTORE_PASSWORD, clientTrustStore);
        // Create CRL file with a revoked certificate
        crlRevoked = Paths.get(tempDir, "revoked.crl").toString();
        certUtils.createCRL(clientCaCert, clientCaCertKey, crlRevoked, true, false);
        // Create CRL file with a non-revoked certificate
        crlNotRevoked = Paths.get(tempDir, "notRevoked.crl").toString();
        certUtils.createCRL(clientCaCert, clientCaCertKey, crlNotRevoked, false, false);
        // Create CRL file with a duplicated non-revoked certificate entry
        crlNotRevokedDuplicate = Paths.get(tempDir, "notRevokedDuplicate.crl").toString();
        certUtils.createCRL(clientCaCert, clientCaCertKey, crlNotRevokedDuplicate, false, true);
        // Create a corrupted CRL file
        crlCorrupted = Paths.get(tempDir, "corrupted.crl").toString();
        certUtils.createCorruptedCRL(crlCorrupted);
    }

}
