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
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.glassfish.jersey.server.ChunkedOutput;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
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
    @Path("/chain")
    @ApiOperation(value = "Performs the given operation chain on the graph", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation chain"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "One or more of the requested operations are not supported by the target store")})
    Response execute(final OperationChain opChain);

    @POST
    @Path("/chunked")
    @ApiOperation(value = "Performs the given operation on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 202, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    ChunkedOutput<String> executeChunked(final Operation operation);

    @POST
    @Path("/chain/chunked")
    @ApiOperation(value = "Performs the given operation chain on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 202, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation chain"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "One or more of the requested operations are not supported by the target store")})
    ChunkedOutput<String> executeChunked(final OperationChain<CloseableIterable<Element>> opChain);

    @POST
    @Path("/generate/objects")
    @ApiOperation(value = "Generate objects from elements", response = Object.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation chain"),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response generateObjects(final GenerateObjects<Object> operation);

    @POST
    @Path("/generate/elements")
    @ApiOperation(value = "Generate elements from objects", response = Element.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation chain"),
            @ApiResponse(code = 500, message = "Something went wrong in the server")})
    Response generateElements(final GenerateElements<Object> operation);

    @POST
    @Path("/adjSeeds")
    @ApiOperation(value = "Gets adjacent entity seeds", response = EntityId.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response getAdjacentIds(final GetAdjacentIds operation);

    @POST
    @Path("/elements/all")
    @ApiOperation(value = "Gets all elements", response = Element.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response getAllElements(final GetAllElements operation);

    @POST
    @Path("/elements")
    @ApiOperation(value = "Gets elements", response = Element.class, responseContainer = "List", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response getElements(final GetElements operation);

    @PUT
    @Path("/elements")
    @ApiOperation(value = "Add elements to the graph", response = Boolean.class, produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "Error while processing request body"),
            @ApiResponse(code = 403, message = "The current user cannot perform the requested operation"),
            @ApiResponse(code = 500, message = "Something went wrong in the server"),
            @ApiResponse(code = 501, message = "The requested operation is not supported by the target store")})
    Response addElements(final AddElements operation);
}

