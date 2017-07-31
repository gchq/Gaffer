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
import org.glassfish.jersey.server.ChunkedOutput;
import uk.gov.gchq.gaffer.operation.Operation;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * An <code>IOperationServiceV2</code> has methods to execute {@link uk.gov.gchq.gaffer.operation.Operation}s on the
 * {@link uk.gov.gchq.gaffer.graph.Graph}.
 */
@Path("/graph/operations")
@Api(value = "operations")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
public interface IOperationServiceV2 {

    @GET
    @ApiOperation(value = "Gets all operations supported by the store.", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Response getOperations();

    @POST
    @ApiOperation(value = "Performs the given operation on the graph", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response execute(final Operation operation);

    @POST
    @Path("/chunked")
    @ApiOperation(value = "Performs the given operation on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 202, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    ChunkedOutput<String> executeChunked(final Operation operation);

    @GET
    @Path("/{className}")
    @ApiOperation(value = "Gets details about the specified operation class.", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 403, message = "The current user cannot access the requested operation"),
            @ApiResponse(code = 404, message = "Operation not found"),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response operationDetails(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className) throws InstantiationException, IllegalAccessException;

    @POST
    @Path("/{className}")
    @ApiOperation(value = "Performs the given operation on the graph", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response executeOperation(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className, final Operation operation);

    @GET
    @Path("/{className}/doc")
    @ApiOperation(value = "Gets documentation relating to the specified operation class.", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 403, message = "The current user cannot access the requested operation"),
            @ApiResponse(code = 404, message = "Operation not found"),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response operationDocumentation(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className);

    @GET
    @Path("/{className}/example")
    @ApiOperation(value = "Gets example JSON for the specified operation class.", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 403, message = "The current user cannot access the requested operation"),
            @ApiResponse(code = 404, message = "Operation not found"),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response operationExample(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className) throws InstantiationException, IllegalAccessException;

    @GET
    @Path("/{className}/next")
    @ApiOperation(value = "Gets all the compatible operations that could be added to an operation chain after the provided operation.",
            response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 404, message = "Operation not found."),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response nextOperations(@ApiParam(value = "the fully qualified class name") @PathParam("className") final String className);

}

