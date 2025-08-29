/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
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

import com.ericsson.bss.cassandra.ecchronos.core.exceptions.EcChronosException;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairInfo;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.RepairStats;
import com.ericsson.bss.cassandra.ecchronos.core.utils.RepairStatsProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.ReplicatedTableProvider;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReferenceFactory;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.REPAIR_MANAGEMENT_ENDPOINT_PREFIX;
import static com.ericsson.bss.cassandra.ecchronos.rest.RestUtils.getDefaultDurationOrProvided;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * When updating the path it should also be updated in the OSGi component.
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

    public RepairManagementRESTImpl(final TableReferenceFactory tableReferenceFactory,
                                    final ReplicatedTableProvider replicatedTableProvider,
                                    final RepairStatsProvider repairStatsProvider)
    {
        myTableReferenceFactory = tableReferenceFactory;
        myReplicatedTableProvider = replicatedTableProvider;
        myRepairStatsProvider = repairStatsProvider;
    }

    @Override
    @GetMapping(value = REPAIR_MANAGEMENT_ENDPOINT_PREFIX + "/repairInfo", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(operationId = "get-repair-info",
            description = "Get repair information, if keyspace and table are provided while duration and since are"
                    + " not, the duration will default to GC_GRACE_SECONDS of the table. "
                    + "This operation might take time depending on the provided params since it's based on "
                    + "the repair history.",
            summary = "Get repair information")
    public final ResponseEntity<RepairInfo> getRepairInfo(
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
            final Duration duration,
            @RequestParam(required = false)
            @Parameter(description = "Decides if the repair-info should be calculated for the local node only.")
            final boolean isLocal)
    {
        return ResponseEntity.ok(fetchRepairInfo(keyspace, table, since, duration, isLocal));
    }

    private RepairInfo fetchRepairInfo(final String keyspace, final String table,
            final Long since, final Duration duration, final boolean isLocal)
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
                    repairInfo = createRepairInfo(Collections.singleton(tableReference), since, actualDuration,
                            isLocal);
                }
                else
                {
                    repairInfo = createRepairInfo(myTableReferenceFactory.forKeyspace(keyspace), since, actualDuration,
                            isLocal);
                }
            }
            else
            {
                if (table != null)
                {
                    throw new ResponseStatusException(BAD_REQUEST, "Keyspace must be provided if table is provided");
                }
                repairInfo = createRepairInfo(myTableReferenceFactory.forCluster(), since, actualDuration, isLocal);
            }
            return repairInfo;
        }
        catch (EcChronosException e)
        {
            throw new ResponseStatusException(NOT_FOUND, NOT_FOUND.getReasonPhrase(), e);
        }
    }

    private RepairInfo createRepairInfo(final Set<TableReference> tables, final Long since,
            final Duration duration, final boolean isLocal)
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

        long finalToTime = toTime;
        List<RepairStats> repairStats = tables.stream()
                .filter(table -> myReplicatedTableProvider.accept(table.getKeyspace()))
                .map(table -> myRepairStatsProvider.getRepairStats(table, sinceTime, finalToTime, isLocal))
                .toList();
        return new RepairInfo(sinceTime, toTime, repairStats);
    }
}
