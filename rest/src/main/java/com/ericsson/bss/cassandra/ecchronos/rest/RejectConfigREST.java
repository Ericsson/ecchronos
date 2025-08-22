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

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicy;
import com.ericsson.bss.cassandra.ecchronos.core.TimeBasedRunPolicyBucket;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.ArrayList;
import java.util.List;
import org.springframework.web.server.ResponseStatusException;


@Tag(name = "Reject-Configuration", description = "Management of reject repairs per keyspace, table and datacenter")
@RestController
public class RejectConfigREST
{
    @Autowired
    private final TimeBasedRunPolicy myTimeBasedRunPolicy;

    public RejectConfigREST(final TimeBasedRunPolicy timeBasedRunPolicy)
    {
        myTimeBasedRunPolicy = timeBasedRunPolicy;
    }

    @GetMapping(value = "/rejections")
    @Operation(operationId = "get-rejections",
            description = "Get rejection information, if keyspace and table are provided it will return the data"
            + "filtered by the specified keyspace and table",
            summary = "Get rejection information")
    public final ResponseEntity<List<TimeBasedRunPolicyBucket>> getAllRejections(
            @RequestParam(required = false)
            @Parameter(description = "Only return rejections matching the keyspace, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "Only return rejections matching the table, mandatory if 'keyspace' is provided.")
            final String table
    )
    {
        return ResponseEntity.ok(getRejections(keyspace, table));
    }

    @PostMapping(value = "/rejections", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "create-rejection",
            description = "Create rejection information",
            summary = "Create rejection information")
    public final ResponseEntity<Map<String, Object>> addRejectConfig(@RequestBody final TimeBasedRunPolicyBucket bucket)
    {
        try
        {
            ResultSet rs = myTimeBasedRunPolicy.addRejection(bucket);

            Map<String, Object> response = createDefaultResponse(rs,
                    "Rejection created successfully.",
                    "Rejection already exists in the table.");

            return ResponseEntity.ok(response);

        }
        catch (AllNodesFailedException | QueryExecutionException | QueryValidationException e)
        {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Failed to add Rejection", e);
        }
    }


    @PatchMapping(value = "/rejections", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "update-rejection",
            description = "Update rejection information, it will add a new datacenter in "
            + "dc_exclusion column based on the Primary Key data provided.",
            summary = "Update rejection information")
    public final ResponseEntity<Map<String, Object>> addDatacenterExclusion(
            @RequestBody final TimeBasedRunPolicyBucket  bucket)
    {
        try
        {
            ResultSet rs = myTimeBasedRunPolicy.addDatacenterExclusion(bucket);

            Map<String, Object> response  = createDefaultResponse(rs,
                    "Datacenter Exclusion added successfully.",
                    "Failed to add Datacenter Exclusion");

            return ResponseEntity.ok(response);

        }
        catch (AllNodesFailedException | QueryExecutionException | QueryValidationException e)
        {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Failed to add Rejection", e);
        }
    }

    @DeleteMapping(value = "/rejections", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "delete-rejection",
            description = "Delete rejection based on Primary Key data provided, "
            + "if dc_exclusion is provided, it will remove the datacenter list from dc_exclusion column",
            summary = "Delete rejection information")
    public final ResponseEntity<Map<String, Object>> delete(@RequestBody final TimeBasedRunPolicyBucket  bucket)
    {
        ResultSet rs;
        try
        {
            if (!bucket.dcExclusions().isEmpty())
            {
                rs = myTimeBasedRunPolicy.dropDatacenterExclusion(bucket);
            }
            else
            {
                rs =  myTimeBasedRunPolicy.deleteRejection(bucket);
            }
            Map<String, Object> response  = createDefaultResponse(rs,
                    "Rejection deleted successfully.",
                    "Failed to remove rejection");

            return ResponseEntity.ok(response);

        }
        catch (AllNodesFailedException | QueryExecutionException | QueryValidationException e)
        {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Failed to add Rejection", e);
        }
    }

    @DeleteMapping(value = "/rejections/all", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "delete-all-rejections",
            description = "Delete all the data related with rejections.",
            summary = "Delete all rejection information")
    public final ResponseEntity<Map<String, Object>> deleteAllRejections()
    {
        try
        {
            ResultSet rs = myTimeBasedRunPolicy.truncate();

            Map<String, Object> response = createDefaultResponse(
                    rs, "Rejections table truncated successfully.",
                    "Unable to Truncate table.");

            return ResponseEntity.ok(response);

        }
        catch (AllNodesFailedException | QueryExecutionException | QueryValidationException e)
        {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Failed to truncate rejections table", e);
        }
    }


    public final List<TimeBasedRunPolicyBucket> getRejections(
            final String keyspaceName,
            final String tableName)
    {
        if (keyspaceName != null)
        {
            if (tableName != null)
            {
                return toBucket(myTimeBasedRunPolicy.getRejectionsByKsAndTb(keyspaceName, tableName));
            }
            return toBucket(myTimeBasedRunPolicy.getRejectionsByKs(keyspaceName));
        }
        return toBucket(myTimeBasedRunPolicy.getAllRejections());
    }

    public final List<TimeBasedRunPolicyBucket> toBucket(final ResultSet rs)
    {
        List<TimeBasedRunPolicyBucket> rejections = new ArrayList<>();
        for (Row row : rs)
        {
            TimeBasedRunPolicyBucket bucket = new TimeBasedRunPolicyBucket(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    row.getInt("start_hour"),
                    row.getInt("start_minute"),
                    row.getInt("end_hour"),
                    row.getInt("end_minute"),
                    row.getSet("dc_exclusion", String.class)
            );
            rejections.add(bucket);
        }
        return rejections;
    }

    private Map<String, Object> createDefaultResponse(
            final ResultSet rs,
            final String success,
            final String failed)
    {
        List<TimeBasedRunPolicyBucket> body = getRejections(null, null);
        Map<String, Object> response = new HashMap<>();
        response.put("data", body);
        response.put("message", rs.wasApplied()
                ? success
                : failed);

        return response;
    }
}
