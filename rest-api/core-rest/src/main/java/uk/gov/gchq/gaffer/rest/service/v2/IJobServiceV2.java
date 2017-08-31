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

package uk.gov.gchq.gaffer.rest.service.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * An <code>IJobService</code> handles jobs - executing Jobs and getting Job
 * statuses.
 */
@Path("/graph/jobs")
@Api(value = "job")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public interface IJobServiceV2 {

    @POST
    @ApiOperation(value = "Performs the given operation chain job on the graph", response = JobDetail.class, code = 201, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 201, message = "A new job was successfully submitted"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store"),
            @ApiResponse(code = 503, message = "The job service is not available.")})
    Response executeJob(final OperationChain opChain) throws OperationException;

    @GET
    @ApiOperation(value = "Get the details of all jobs", response = JobDetail.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 503, message = "The job service is not available.")})
    Response details() throws OperationException;

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get the details of a job", response = JobDetail.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 404, message = "Job was not found."),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 503, message = "The job service is not available.")})
    Response details(@ApiParam(value = "a job id") @PathParam("id") final String id) throws OperationException;

    @GET
    @Path("{id}/results")
    @ApiOperation(value = "Get the results of a job", response = Object.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 503, message = "The job service is not available.")})
    Response results(@ApiParam(value = "a job id") @PathParam("id") final String id) throws OperationException;
}
