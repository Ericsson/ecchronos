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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicyBucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.ResponseEntity;


@RunWith(MockitoJUnitRunner.Silent.class)
public class TestRejectConfigREST
{
    @Mock
    private TimeBasedRunPolicy myTimeBasedRunPolicy;

    private RejectConfigREST rejectConfigREST;

    private ResultSet rsMock;
    private Row rowMock;

    @Before
    public void setup()
    {
        rejectConfigREST = new RejectConfigREST(myTimeBasedRunPolicy);

        rowMock = mock(Row.class);
        when(rowMock.getString("keyspace_name")).thenReturn("ks");
        when(rowMock.getString("table_name")).thenReturn("tb");
        when(rowMock.getInt("start_hour")).thenReturn(0);
        when(rowMock.getInt("start_minute")).thenReturn(0);
        when(rowMock.getInt("end_hour")).thenReturn(1);
        when(rowMock.getInt("end_minute")).thenReturn(0);
        when(rowMock.getSet("dc_exclusion", String.class)).thenReturn(new HashSet<>());

        rsMock = mock(ResultSet.class);
        when(rsMock.iterator()).thenReturn(Collections.singletonList(rowMock).iterator());
        when(rsMock.wasApplied()).thenReturn(true);

        when(myTimeBasedRunPolicy.getAllRejections()).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.getRejectionsByKs(anyString())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.getRejectionsByKsAndTb(anyString(), anyString())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.addRejection(any())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.addDatacenterExclusion(any())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.deleteRejection(any())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.dropDatacenterExclusion(any())).thenReturn(rsMock);
        when(myTimeBasedRunPolicy.truncate()).thenReturn(rsMock);
    }

    @Test
    public void testGetAllRejections_NoFilters()
    {
        ResponseEntity<List<TimeBasedRunPolicyBucket>> response = rejectConfigREST.getAllRejections(null, null);
        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertFalse(response.getBody().isEmpty());
    }

    @Test
    public void testGetAllRejections_WithKeyspaceOnly()
    {
        ResponseEntity<List<TimeBasedRunPolicyBucket>> response = rejectConfigREST.getAllRejections("ks", null);
        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertFalse(response.getBody().isEmpty());
    }

    @Test
    public void testGetAllRejections_WithKeyspaceAndTable()
    {
        ResponseEntity<List<TimeBasedRunPolicyBucket>> response = rejectConfigREST.getAllRejections("ks", "tb");
        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertFalse(response.getBody().isEmpty());
    }

    @Test
    public void testAddRejectConfig_Success()
    {
        TimeBasedRunPolicyBucket bucket = new TimeBasedRunPolicyBucket("ks", "tb", 0, 0, 1, 0, new HashSet<>());
        ResponseEntity<Map<String, Object>> response = rejectConfigREST.addRejectConfig(bucket);

        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertEquals("Rejection created successfully.", response.getBody().get("message"));
        assertNotNull(response.getBody().get("data"));
    }

    @Test
    public void testAddDatacenterExclusion_Success()
    {
        TimeBasedRunPolicyBucket bucket = new TimeBasedRunPolicyBucket("ks", "tb", 0, 0, 1, 0, new HashSet<>());
        ResponseEntity<Map<String, Object>> response = rejectConfigREST.addDatacenterExclusion(bucket);

        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertEquals("Datacenter Exclusion added successfully.", response.getBody().get("message"));
        assertNotNull(response.getBody().get("data"));
    }

    @Test
    public void testDelete_WithDcExclusions()
    {
        Set<String> dcExclusions = new HashSet<>();
        dcExclusions.add("dc1");
        TimeBasedRunPolicyBucket bucket = new TimeBasedRunPolicyBucket("ks", "tb", 0, 0, 1, 0, dcExclusions);

        ResponseEntity<Map<String, Object>> response = rejectConfigREST.delete(bucket);

        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertEquals("Rejection deleted successfully.", response.getBody().get("message"));
        assertNotNull(response.getBody().get("data"));
    }

    @Test
    public void testDelete_WithoutDcExclusions()
    {
        TimeBasedRunPolicyBucket bucket = new TimeBasedRunPolicyBucket("ks", "tb", 0, 0, 1, 0, new HashSet<>());

        ResponseEntity<Map<String, Object>> response = rejectConfigREST.delete(bucket);

        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertEquals("Rejection deleted successfully.", response.getBody().get("message"));
        assertNotNull(response.getBody().get("data"));
    }

    @Test
    public void testDeleteAllRejections_Success()
    {
        ResponseEntity<Map<String, Object>> response = rejectConfigREST.deleteAllRejections();

        assertNotNull(response);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertEquals("Rejections table truncated successfully.", response.getBody().get("message"));
        assertNotNull(response.getBody().get("data"));
    }

}
