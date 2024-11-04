/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.impl.locks;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.locks.LockFactory.DistributedLock;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestLockCache
{
    private static final String DATA_CENTER = "DC1";
    private static final String RESOURCE = "RepairResource-91e32362-7af4-11e9-8f9e-2a86e4085a59-1";
    private static final int PRIORITY = 1;
    private static final Map<String, String> METADATA = new HashMap<>();

    @Mock
    private LockCache.LockSupplier mockedLockSupplier;

    private LockCache myLockCache;

    @Before
    public void setup()
    {
        myLockCache = new LockCache(mockedLockSupplier, 30L);
    }

    @Test
    public void testGetLock() throws LockException
    {
        DistributedLock expectedLock = doReturnLockOnGetLock();

        assertGetLockRetrievesExpectedLock(expectedLock);
    }

    @Test
    public void testGetThrowingLockIsCached() throws LockException
    {
        LockException expectedExcetion = doThrowOnGetLock();

        assertGetLockThrowsException(expectedExcetion);

        // Reset return type, locking should still throw
        doReturnLockOnGetLock();

        assertGetLockThrowsException(expectedExcetion);
    }

    @Test
    public void testGetMultipleLocks() throws LockException
    {
        String otherResource = "RepairResource-b2e33e60-7af6-11e9-8f9e-2a86e4085a59-1";

        DistributedLock expectedLock = doReturnLockOnGetLock(RESOURCE);
        DistributedLock expectedOtherLock = doReturnLockOnGetLock(otherResource);

        assertGetLockRetrievesExpectedLock(RESOURCE, expectedLock);
        assertGetLockRetrievesExpectedLock(otherResource, expectedOtherLock);
    }

    @Test
    public void testGetOtherLockAfterThrowingOnAnotherResource() throws LockException
    {
        String otherResource = "RepairResource-b2e33e60-7af6-11e9-8f9e-2a86e4085a59-1";

        LockException expectedException = doThrowOnGetLock(RESOURCE);
        DistributedLock expectedOtherLock = doReturnLockOnGetLock(otherResource);

        assertGetLockThrowsException(RESOURCE, expectedException);
        assertGetLockRetrievesExpectedLock(otherResource, expectedOtherLock);
    }

    @Test
    public void testGetLockAfterCachedExceptionHasExpired() throws LockException, InterruptedException
    {
        myLockCache = new LockCache(mockedLockSupplier, 20, TimeUnit.MILLISECONDS);

        LockException expectedException = doThrowOnGetLock();
        assertGetLockThrowsException(expectedException);

        Thread.sleep(20);

        DistributedLock expectedLock = doReturnLockOnGetLock();
        assertGetLockRetrievesExpectedLock(expectedLock);
    }

    @Test
    public void testEqualsContract()
    {
        EqualsVerifier.forClass(LockCache.LockKey.class).usingGetClass().verify();
    }

    private void assertGetLockRetrievesExpectedLock(DistributedLock expectedLock) throws LockException
    {
        assertGetLockRetrievesExpectedLock(RESOURCE, expectedLock);
    }

    private void assertGetLockRetrievesExpectedLock(String resource, DistributedLock expectedLock) throws LockException
    {
        assertThat(myLockCache.getLock(DATA_CENTER, resource, PRIORITY, METADATA)).isSameAs(expectedLock);
        assertThat(myLockCache.getCachedFailure(DATA_CENTER, resource)).isEmpty();
    }

    private void assertGetLockThrowsException(LockException expectedException)
    {
        assertGetLockThrowsException(RESOURCE, expectedException);
    }

    private void assertGetLockThrowsException(String resource, LockException expectedException)
    {
        assertThatThrownBy(() -> myLockCache.getLock(DATA_CENTER, resource, PRIORITY, METADATA)).isSameAs(expectedException);
        assertThat(myLockCache.getCachedFailure(DATA_CENTER, resource)).isNotEmpty();
    }

    private DistributedLock doReturnLockOnGetLock() throws LockException
    {
        return doReturnLockOnGetLock(RESOURCE);
    }

    private DistributedLock doReturnLockOnGetLock(String resource) throws LockException
    {
        DistributedLock expectedLock = mock(DistributedLock.class);
        when(mockedLockSupplier.getLock(eq(DATA_CENTER), eq(resource), eq(PRIORITY), eq(METADATA))).thenReturn(expectedLock);
        return expectedLock;
    }

    private LockException doThrowOnGetLock() throws LockException
    {
        return doThrowOnGetLock(RESOURCE);
    }

    private LockException doThrowOnGetLock(String resource) throws LockException
    {
        LockException expectedException = new LockException("");
        when(mockedLockSupplier.getLock(eq(DATA_CENTER), eq(resource), eq(PRIORITY), eq(METADATA))).thenThrow(expectedException);
        return expectedException;
    }
}