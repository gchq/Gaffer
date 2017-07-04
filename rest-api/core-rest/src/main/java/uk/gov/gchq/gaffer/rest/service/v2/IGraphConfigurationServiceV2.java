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
import uk.gov.gchq.gaffer.store.schema.Schema;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.Set;

import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_MEDIA_TYPE_V2;

/**
 * An <code>IGraphConfigurationService</code> has methods to get {@link uk.gov.gchq.gaffer.graph.Graph} configuration information
 * such as the {@link uk.gov.gchq.gaffer.store.schema.Schema} and available {@link uk.gov.gchq.gaffer.operation.Operation}s.
 */
@Path("/graph/config")
@Produces(GAFFER_MEDIA_TYPE_V2)
@Api(value = "/graph", description = "Methods to get graph configuration information.")
public interface IGraphConfigurationServiceV2 {

    @GET
    @ApiOperation(value = "Gets the graph configuration details", response = String.class)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    String getConfigurationDetails();

    @GET
    @Path("/schema")
    @ApiOperation(value = "Gets the schema", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Schema getSchema();

    @GET
    @Path("/filterFunctions")
    @ApiOperation(value = "Gets available filter functions. See <a href='https://github.com/gchq/Gaffer/wiki/filter-function-examples' target='_blank' style='text-decoration: underline;'>Wiki</a>.", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getFilterFunctions();

    @GET
    @Path("/filterFunctions/{inputClass}")
    @ApiOperation(value = "Gets available filter functions for the given input class is provided.  See <a href='https://github.com/gchq/Gaffer/wiki/filter-function-examples' target='_blank' style='text-decoration: underline;'>Wiki</a>.", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getFilterFunctions(@ApiParam(value = "a function input java class") @PathParam("inputClass") final String inputClass);

    @GET
    @Path("/transformFunctions")
    @ApiOperation(value = "Gets available transform functions", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getTransformFunctions();

    @GET
    @Path("/elementGenerators")
    @ApiOperation(value = "Gets available element generators", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getElementGenerators();

    @GET
    @Path("/objectGenerators")
    @ApiOperation(value = "Gets available object generators", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getObjectGenerators();

    @GET
    @Path("/storeTraits")
    @ApiOperation(value = "Gets all supported store traits", response = StoreTrait.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<StoreTrait> getStoreTraits();

    @GET
    @Path("/nextOperations/{className}")
    @ApiOperation(value = "Gets all the compatible operations that could be added to an operation chain after the provided operation.",
            response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<Class> getNextOperations(@ApiParam(value = "an operation class name") @PathParam("className") final String operationClassName);

    @POST
    @Path("/isOperationSupported")
    @ApiOperation(value = "Determines whether the operation type supplied is supported by the store",
            response = Boolean.class)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Boolean isOperationSupported(final Class operation);

    @GET
    @Path("/serialisedFields/{className}")
    @ApiOperation(value = "Gets all serialised fields for a given java class.", response = String.class, responseContainer = "list")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    Set<String> getSerialisedFields(@ApiParam(value = "a java class name") @PathParam("className") final String className);
}
