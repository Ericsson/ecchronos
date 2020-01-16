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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Repair scheduler rest interface.
 *
 * Whenever the interface is changed it must be reflected in docs.
 */
public interface RepairSchedulerREST
{
    /**
     * Get details of a specific table repair job.
     *
     * @param keyspace The keyspace of the table
     * @param table The table to get details of
     * @return A JSON representation of {@link CompleteRepairJob}
     */
    @GET
    @Path("/get/{keyspace}/{table}")
    @Produces(MediaType.APPLICATION_JSON)
    String get(@PathParam("keyspace") String keyspace,
               @PathParam("table") String table);

    /**
     * Get a list of the status of all repair jobs.
     *
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    @GET
    @Path("/list")
    @Produces(MediaType.APPLICATION_JSON)
    String list();

    /**
     * Get a list of the status of all repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace to list
     * @return A list of JSON representations of {@link ScheduledRepairJob}
     */
    @GET
    @Path("/list/{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    String listKeyspace(@PathParam("keyspace") String keyspace);

    /**
     * Get a list of the configuration of all repair jobs.
     *
     * @return A configuration list of JSON representations of {@link TableRepairConfig}
     */
    @GET
    @Path("/config")
    @Produces(MediaType.APPLICATION_JSON)
    String config();

    /**
     * Get a list of the configuration of all repair jobs for a specific keyspace.
     *
     * @param keyspace The keyspace
     * @return A configuration list of JSON representations of {@link TableRepairConfig}
     */
    @GET
    @Path("/config/{keyspace}")
    @Produces(MediaType.APPLICATION_JSON)
    String configKeyspace(@PathParam("keyspace") String keyspace);
}
