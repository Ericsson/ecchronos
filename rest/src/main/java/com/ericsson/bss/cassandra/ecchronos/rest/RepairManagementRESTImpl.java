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
package com.ericsson.bss.cassandra.ecchronos.rest;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.ericsson.bss.cassandra.ecchronos.connection.DistributedNativeConnectionProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.table.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.table.TableReferenceFactory;
import com.ericsson.bss.cassandra.ecchronos.utils.exceptions.EcChronosException;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.getDefaultDurationOrProvided;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.parseIdOrThrow;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * REST Controller for managing repair operations in ecChronos, providing endpoints to fetch repair statistics
 * based on Cassandra tables, keyspaces, and time ranges.
 */
@Tag(name = "Repair-Management", description = "Management of repairs")
@RestController
@OpenAPIDefinition(info = @Info(
        title = "REST API",
        license = @License(
                name = "Apache 2.0",
                url = "https://www.apache.org/licenses/LICENSE-2.0"),
        version = "1.0.0"))
public class RepairManagementRESTImpl implements RepairManagementREST
{
    @Autowired
    private final TableReferenceFactory myTableReferenceFactory;

    @Autowired
    private final ReplicatedTableProvider myReplicatedTableProvider;

    @Autowired
    private final RepairStatsProvider myRepairStatsProvider;

    @Autowired
    private final DistributedNativeConnectionProvider myDistributedNativeConnectionProvider;

    public RepairManagementRESTImpl(
            final TableReferenceFactory tableReferenceFactory,
            final ReplicatedTableProvider replicatedTableProvider,
            final RepairStatsProvider repairStatsProvider,
            final DistributedNativeConnectionProvider nativeConnectionProvider)
    {
        myTableReferenceFactory = tableReferenceFactory;
        myReplicatedTableProvider = replicatedTableProvider;
        myRepairStatsProvider = repairStatsProvider;
        myDistributedNativeConnectionProvider = nativeConnectionProvider;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairInfo/{nodeID}", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repair-info",
            description = "Get repair information, if keyspace and table are provided while duration and since are"
                    + " not, the duration will default to GC_GRACE_SECONDS of the table. "
                    + "This operation might take time depending on the provided params since it's based on "
                    + "the repair history.",
            summary = "Get repair information")
    public final ResponseEntity<RepairInfo> getRepairInfo(
            @PathVariable()
            @Parameter(description = "Return repair-info matching the nodeID, mandatory parameter.")
            final String nodeID,
            @RequestParam(required = false)
            @Parameter(description = "Only return repair-info matching the keyspace, mandatory if 'table' is provided.")
            final String keyspace,
            @RequestParam(required = false)
            @Parameter(description = "Only return repair-info matching the table.")
            final String table,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            @Parameter(description = "Since time, can be specified as ISO8601 date or as milliseconds since epoch."
                    + " Required if keyspace and table or duration is not specified.",
                    schema = @Schema(type = "string"))
            final Long since,
            @RequestParam(required = false)
            @Parameter(description = "Duration, can be specified as either a simple duration like"
                    + " '30s' or as ISO8601 duration 'pt30s'."
                    + " Required if keyspace and table or since is not specified.",
                    schema = @Schema(type = "string"))
            final Duration duration)
    {
        UUID uuid = parseIdOrThrow(nodeID);
        return ResponseEntity.ok(fetchRepairInfo(uuid, keyspace, table, since, duration));
    }

    private RepairInfo fetchRepairInfo(
            final UUID nodeID,
            final String keyspace, final String table,
            final Long since, final Duration duration
    )
    {
        try
        {
            RepairInfo repairInfo;
            Duration actualDuration = duration;
            if (keyspace != null)
            {
                if (table != null)
                {
                    TableReference tableReference = myTableReferenceFactory.forTable(keyspace, table);
                    if (tableReference == null)
                    {
                        throw new ResponseStatusException(NOT_FOUND,
                                "Table " + keyspace + "." + table + " does not exist");
                    }
                    actualDuration = getDefaultDurationOrProvided(tableReference, duration, since);
                    repairInfo = createRepairInfo(nodeID, Collections.singleton(tableReference), since, actualDuration);
                }
                else
                {
                    repairInfo = createRepairInfo(nodeID, myTableReferenceFactory.forKeyspace(keyspace), since, actualDuration);
                }
            }
            else
            {
                if (table != null)
                {
                    throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
                }
                repairInfo = createRepairInfo(nodeID, myTableReferenceFactory.forCluster(), since, actualDuration);
            }
            return repairInfo;
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private RepairInfo createRepairInfo(
            final UUID nodeID,
            final Set<TableReference> tables, final Long since,
            final Duration duration
    )
    {
        long toTime = System.currentTimeMillis();
        long sinceTime;
        if (since != null)
        {
            sinceTime = since.longValue();
            if (duration != null)
            {
                toTime = sinceTime + TimeUnit.SECONDS.toMillis(duration.getSeconds());
            }
        }
        else if (duration != null)
        {
            sinceTime = toTime - TimeUnit.SECONDS.toMillis(duration.getSeconds());
        }
        else
        {
            throw new ResponseStatusException(BAD_REQUEST, "Since or duration or both must be specified");
        }
        if (toTime < sinceTime)
        {
            throw new ResponseStatusException(BAD_REQUEST, "'to' (" + toTime + ") is before 'since' ("
                    + sinceTime + ")");
        }

        List<RepairStats> repairStats = new ArrayList<>();
        Node node = myDistributedNativeConnectionProvider.getNodes().get(nodeID);
        for (TableReference table : tables)
        {
            if (myReplicatedTableProvider.accept(node, table.getKeyspace()))
            {
                repairStats.add(myRepairStatsProvider.getRepairStats(nodeID, table, sinceTime, toTime));
            }
        }
        return new RepairInfo(sinceTime, toTime, repairStats);
    }
}

