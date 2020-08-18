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

import com.ericsson.bss.cassandra.ecchronos.core.repair.types.CompleteRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.ScheduledRepairJob;
import com.ericsson.bss.cassandra.ecchronos.core.repair.types.TableRepairConfig;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Repair scheduler rest interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface RepairManagementREST
{
    /**
     * Get a list of the status of all scheduled repair jobs.
     *
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    String status();

    /**
     * Get a list of the status of all scheduled repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace to list
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    @GET
    @Path("/status/keyspaces/{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    String keyspaceStatus(@PathParam("keyspace") String keyspace); // NOPMD

    /**
     * Get a list of the status of all scheduled repair jobs for a specific table.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get status of
     * @return A JSON representation of {@link ScheduledRepairJob}
     */
    @GET
    @Path("/status/keyspaces/{keyspace}/tables/{table}")
    @Produces(MediaType.APPLICATION_JSON)
    String tableStatus(@PathParam("keyspace") String keyspace, // NOPMD
                       @PathParam("table") String table); // NOPMD

    /**
     * Get status of a specific scheduled table repair job.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get status of
     * @param id The id of the job
     * @return A JSON representation of {@link CompleteRepairJob}
     */
    @GET
    @Path("/status/keyspaces/{keyspace}/tables/{table}/ids/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    String jobStatus(@PathParam("keyspace") String keyspace, // NOPMD
                     @PathParam("table") String table, // NOPMD
                     @PathParam("id") String id);

    /**
     * Get a list of configuration of all scheduled repair jobs.
     *
     * @return A list of JSON representations of {@link TableRepairConfig}
     */
    @GET
    @Path("/config")
    @Produces(MediaType.APPLICATION_JSON)
    String config(); // NOPMD

    /**
     * Get a list of configuration of all scheduled repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace to list
     * @return A list of JSON representations of {@link TableRepairConfig}
     */
    @GET
    @Path("/config/keyspaces/{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    String keyspaceConfig(@PathParam("keyspace") String keyspace); // NOPMD

    /**
     * Get configuration of a specific scheduled table repair job.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get configuration of
     * @return A JSON representation of {@link TableRepairConfig}
     */
    @GET
    @Path("/config/keyspaces/{keyspace}/tables/{table}")
    @Produces(MediaType.APPLICATION_JSON)
    String tableConfig(@PathParam("keyspace") String keyspace, // NOPMD
                       @PathParam("table") String table); // NOPMD

    /**
     * Schedule an on demand repair to be run on a specific table
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get configuration of
     * @return A JSON representation of {@link ScheduledRepairJob}
     */
    @POST
    @Path("/schedule/keyspaces/{keyspace}/tables/{table}")
    @Produces(MediaType.APPLICATION_JSON)
    String scheduleJob(@PathParam("keyspace") String keyspace, // NOPMD
                                @PathParam("table") String table); // NOPMD
}
