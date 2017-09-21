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

import uk.gov.gchq.gaffer.store.StoreTrait;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.CLASS_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.FUNCTION_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.OK;

/**
 * An {@code IGraphConfigurationService} has methods to get {@link uk.gov.gchq.gaffer.graph.Graph} configuration information
 * such as the {@link uk.gov.gchq.gaffer.store.schema.Schema} and available {@link uk.gov.gchq.gaffer.operation.Operation}s.
 */
@Path("/graph/config")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Api(value = "config")
public interface IGraphConfigurationServiceV2 {

    @GET
    @Path("/schema")
    @ApiOperation(value = "Gets the schema", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getSchema();

    @GET
    @Path("/filterFunctions")
    @ApiOperation(value = "Gets available filter functions.", response = String.class, responseContainer = "list",
            produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getFilterFunction();

    @GET
    @Path("/filterFunctions/{inputClass}")
    @ApiOperation(value = "Gets available filter functions for the given input class is provided.", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = FUNCTION_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getFilterFunction(@ApiParam(value = "a function input java class") @PathParam("inputClass") final String inputClass);

    @GET
    @Path("/transformFunctions")
    @ApiOperation(value = "Gets available transform functions", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getTransformFunctions();

    @GET
    @Path("/elementGenerators")
    @ApiOperation(value = "Gets available element generators", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getElementGenerators();

    @GET
    @Path("/objectGenerators")
    @ApiOperation(value = "Gets available object generators", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getObjectGenerators();

    @GET
    @Path("/storeTraits")
    @ApiOperation(value = "Gets all supported store traits", response = StoreTrait.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getStoreTraits();

    @GET
    @Path("/serialisedFields/{className}")
    @ApiOperation(value = "Gets all serialised fields for a given java class.", response = String.class, responseContainer = "list", produces = APPLICATION_JSON)
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = CLASS_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getSerialisedFields(@ApiParam(value = "a java class name") @PathParam("className") final String className);
}
