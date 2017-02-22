/*
 * Copyright 2016-2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.rest.service;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * An <code>IAsyncService</code> has methods to execute {@link uk.gov.gchq.gaffer.operation.Operation}s asynchronously on the
 * {@link uk.gov.gchq.gaffer.graph.Graph}.
 */
@Path("/graph/async")
@Api(value = "async", description = "Allows async operations to be executed on the graph. See <a href='https://github.com/gchq/Gaffer/wiki/operation-examples' target='_blank'>Wiki</a>.")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface IAsyncService {

    @POST
    @Path("/doOperation")
    @ApiOperation(value = "Performs the given operation chain on the graph asynchronously", response = String.class)
    JobDetail executeAsync(final OperationChain opChain);

    @GET
    @Path("/status/{id}")
    @ApiOperation(value = "Get an asynchronous job status for the given asynchronous job id", response = JobDetail.class)
    JobDetail status(@ApiParam(value = "a job id") @PathParam("id") final String id);
}
