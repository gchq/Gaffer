/*
 * Copyright 2016-2018 Crown Copyright
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
import io.swagger.annotations.ResponseHeader;

import uk.gov.gchq.gaffer.store.StoreTrait;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.CLASS_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.FUNCTION_NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION;
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
    @ApiOperation(value = "Gets the schema",
            notes = "Returns the full schema for the Store, which should outline the entire list of Elements " +
                    "to be stored in the Graph.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getSchema();

    @GET
    @Path("/filterFunctions")
    @ApiOperation(value = "Gets available filter functions",
            notes = "Returns (in no particular order) the complete list of fully qualified " +
                    "classpaths, of filtering functions that are available to the user.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getFilterFunction();

    @GET
    @Path("/filterFunctions/{inputClass}")
    @ApiOperation(value = "Gets available filter functions for the given input class",
            notes = "Returns a list of the fully qualified classpaths of all filter functions that are applicable to the queried input class.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = FUNCTION_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getFilterFunction(@ApiParam(value = "a function input java class") @PathParam("inputClass") final String inputClass);

    @GET
    @Path("/transformFunctions")
    @ApiOperation(value = "Gets available transform functions",
            notes = "Returns a list of the fully qualified classpaths of all available transform functions, in no particular order.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getTransformFunctions();

    @GET
    @Path("/elementGenerators")
    @ApiOperation(value = "Gets available element generators",
            notes = "Returns a list of the fully qualified class paths of the elementGenerators available.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getElementGenerators();

    @GET
    @Path("/objectGenerators")
    @ApiOperation(value = "Gets available object generators",
            notes = "Returns a list of the fully qualified class paths of the objectGenerators available.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getObjectGenerators();

    @GET
    @Path("/storeTraits")
    @ApiOperation(value = "Gets all supported store traits",
            notes = "Returns a list of traits that the current store can support, " +
                    "such as different types of Aggregation and Filtering.",
            response = StoreTrait.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getStoreTraits();

    @GET
    @Path("/serialisedFields/{className}")
    @ApiOperation(value = "Gets all serialised fields for a given java class",
            notes = "Provides a map of all serialised (ie, not to be ignored) fields, " +
                    "for a given class, alongside information related to the expected " +
                    "type of the class.",
            response = String.class,
            responseContainer = "list",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = CLASS_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getSerialisedFields(@ApiParam(value = "The name of the Java class, for which the serialised fields should be retrieved") @PathParam("className") final String className);

    @GET
    @Path("/serialisedFields/{className}/classes")
    @ApiOperation(value = "Gets all serialised fields and their class type, for a given java class",
            response = String.class,
            responseContainer = "map",
            produces = APPLICATION_JSON,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 404, message = CLASS_NOT_FOUND),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getSerialisedFieldClasses(@ApiParam(value = "A java class name") @PathParam("className") final String className);

    @GET
    @Path("/description")
    @Produces(TEXT_PLAIN)
    @ApiOperation(value = "Gets the Graph description",
            response = String.class,
            produces = TEXT_PLAIN,
            responseHeaders = {
                    @ResponseHeader(name = GAFFER_MEDIA_TYPE_HEADER, description = GAFFER_MEDIA_TYPE_HEADER_DESCRIPTION)
            })
    @ApiResponses(value = {@ApiResponse(code = 200, message = OK),
            @ApiResponse(code = 500, message = INTERNAL_SERVER_ERROR)})
    Response getDescription();
}
