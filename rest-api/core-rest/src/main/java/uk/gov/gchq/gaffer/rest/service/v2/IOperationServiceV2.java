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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.glassfish.jersey.server.ChunkedOutput;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;

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
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OPERATION_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OPERATION_NOT_IMPLEMENTED;

/**
 * An {@code IOperationServiceV2} has methods to execute {@link uk.gov.gchq.gaffer.operation.Operation}s on the
 * {@link uk.gov.gchq.gaffer.graph.Graph}.
 */
@Path("/graph/operations")
@Api(value = "operations")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public interface IOperationServiceV2 {

    @GET
    @ApiOperation(value = "Gets all operations supported by the store.", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK)})
    Response getOperations();

    @POST
    @Path("/execute")
    @ApiOperation(value = "Performs the given operation on the graph", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 400, message = BAD_REQUEST),
            @ApiResponse(code = 403, message = FORBIDDEN),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 501, message = OPERATION_NOT_IMPLEMENTED)})
    Response execute(final Operation operation);

    @POST
    @Path("/execute/chunked")
    @ApiOperation(value = "Performs the given operation on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 202, message = OK),
            @ApiResponse(code = 400, message = BAD_REQUEST),
            @ApiResponse(code = 403, message = FORBIDDEN),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR),
            @ApiResponse(code = 501, message = OPERATION_NOT_IMPLEMENTED)})
    ChunkedOutput<String> executeChunked(final Operation operation);

    @SuppressFBWarnings
    ChunkedOutput<String> executeChunkedChain(final OperationChain opChain);

    @GET
    @Path("/{className}")
    @ApiOperation(value = "Gets details about the specified operation class.", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 403, message = FORBIDDEN),
            @ApiResponse(code = 404, message = OPERATION_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response operationDetails(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className) throws InstantiationException, IllegalAccessException;

    @GET
    @Path("/{className}/example")
    @ApiOperation(value = "Gets example JSON for the specified operation class.", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 403, message = FORBIDDEN),
            @ApiResponse(code = 404, message = OPERATION_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response operationExample(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className) throws InstantiationException, IllegalAccessException;

    @GET
    @Path("/{className}/next")
    @ApiOperation(value = "Gets all the compatible operations that could be added to an operation chain after the provided operation.",
            response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = OPERATION_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response nextOperations(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className);

}

