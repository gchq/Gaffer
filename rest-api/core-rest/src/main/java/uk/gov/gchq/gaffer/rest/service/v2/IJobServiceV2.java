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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.BAD_REQUEST;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.FORBIDDEN;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_CREATED;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_SERVICE_UNAVAILABLE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OPERATION_NOT_IMPLEMENTED;

/**
 * An {@code IJobService} handles jobs - executing Jobs and getting Job
 * statuses.
 */
@Path("/graph/jobs")
@Api(value = "job")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public interface IJobServiceV2 {

    @POST
    @ApiOperation(value = "Submits the given operation job to the graph", response = JobDetail.class, code = 201, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 201, message = JOB_CREATED),
            @ApiResponse(code = 400, message = BAD_REQUEST),
            @ApiResponse(code = 403, message = FORBIDDEN),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 501, message = OPERATION_NOT_IMPLEMENTED),
            @ApiResponse(code = 503, message = JOB_SERVICE_UNAVAILABLE)})
    Response executeJob(final Operation operation) throws OperationException;

    @GET
    @ApiOperation(value = "Get the details of all jobs", response = JobDetail.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 503, message = JOB_SERVICE_UNAVAILABLE)})
    Response details() throws OperationException;

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get the details of a job", response = JobDetail.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = JOB_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 503, message = JOB_SERVICE_UNAVAILABLE)})
    Response details(@ApiParam(value = "a job id") @PathParam("id") final String id) throws OperationException;

    @GET
    @Path("{id}/results")
    @ApiOperation(value = "Get the results of a job", response = Object.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 503, message = JOB_SERVICE_UNAVAILABLE)})
    Response results(@ApiParam(value = "a job id") @PathParam("id") final String id) throws OperationException;
}
