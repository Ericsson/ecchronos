/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core;

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.LockException;
import com.ericsson.bss.cassandra.ecchronos.core.scheduling.LockFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Matchers.eq;
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

    @Mock
    private LockFactory.DistributedLock mockedDistributedLock;

    private LockCache myLockCache;

    @Before
    public void setup()
    {
        myLockCache = new LockCache(mockedLockSupplier);
    }

    @Test
    public void testGetLock() throws LockException
    {
        doReturnLockOnSupply();

        assertExpectedLockIsRetrieved();
    }

    @Test
    public void testGetThrowingLockIsCached() throws LockException
    {
        doThrowOnSupply();

        assertCacheThrowsException();

        // Reset return type, locking should still throw
        doReturnLockOnSupply();

        assertCacheThrowsException();
    }

    @Test
    public void testGetOtherLockAfterThrowing() throws LockException
    {
        String otherResource = "RepairResource-b2e33e60-7af6-11e9-8f9e-2a86e4085a59-1";

        doThrowOnSupply(RESOURCE);
        doReturnLockOnSupply(otherResource);

        assertCacheThrowsException(RESOURCE);
        assertExpectedLockIsRetrieved(otherResource);
    }

    @Test
    public void testGetLockAfterCachedExceptionHasExpired() throws LockException, InterruptedException
    {
        myLockCache = new LockCache(mockedLockSupplier, 20, TimeUnit.MILLISECONDS);

        doThrowOnSupply();
        assertCacheThrowsException();

        Thread.sleep(20);

        doReturnLockOnSupply();
        assertExpectedLockIsRetrieved();
    }

    private void assertExpectedLockIsRetrieved() throws LockException
    {
        assertExpectedLockIsRetrieved(RESOURCE);
    }

    private void assertExpectedLockIsRetrieved(String resource) throws LockException
    {
        assertThat(myLockCache.getLock(DATA_CENTER, resource, PRIORITY, METADATA)).isEqualTo(mockedDistributedLock);
        assertThat(myLockCache.getCachedFailure(DATA_CENTER, resource)).isEmpty();
    }

    private void assertCacheThrowsException()
    {
        assertCacheThrowsException(RESOURCE);
    }

    private void assertCacheThrowsException(String resource)
    {
        assertThatExceptionOfType(LockException.class).isThrownBy(() -> myLockCache.getLock(DATA_CENTER, resource, PRIORITY, METADATA));
        assertThat(myLockCache.getCachedFailure(DATA_CENTER, resource)).isNotEmpty();
    }

    private void doReturnLockOnSupply() throws LockException
    {
        doReturnLockOnSupply(RESOURCE);
    }

    private void doReturnLockOnSupply(String resouce) throws LockException
    {
        when(mockedLockSupplier.getLock(eq(DATA_CENTER), eq(resouce), eq(PRIORITY), eq(METADATA))).thenReturn(mockedDistributedLock);
    }

    private void doThrowOnSupply() throws LockException
    {
        doThrowOnSupply(RESOURCE);
    }

    private void doThrowOnSupply(String resource) throws LockException
    {
        when(mockedLockSupplier.getLock(eq(DATA_CENTER), eq(resource), eq(PRIORITY), eq(METADATA))).thenThrow(new LockException(""));
    }
}
