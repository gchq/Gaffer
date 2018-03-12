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

package uk.gov.gchq.gaffer.rest.service.v1;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import java.util.Set;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * An {@code IGraphConfigurationService} has methods to get {@link uk.gov.gchq.gaffer.graph.Graph} configuration information
 * such as the {@link Schema} and available {@link uk.gov.gchq.gaffer.operation.Operation}s.
 */
@Path("/graph")
@Produces(APPLICATION_JSON)
@Api(value = "/graph")
public interface IGraphConfigurationService {
    @GET
    @Path("/schema")
    @ApiOperation(value = "Gets the schema", response = String.class, responseContainer = "list")
    Schema getSchema();

    @GET
    @Path("/description")
    @ApiOperation(value = "Gets the Graph description", response = String.class)
    String getDescription();

    @GET
    @Path("/filterFunctions")
    @ApiOperation(value = "Gets available filter functions.", response = String.class, responseContainer = "list")
    Set<Class> getFilterFunctions();

    @GET
    @Path("/filterFunctions/{inputClass}")
    @ApiOperation(value = "Gets available filter functions for the given input class is provided.", response = String.class, responseContainer = "list")
    Set<Class> getFilterFunctions(@ApiParam(value = "a function input java class") @PathParam("inputClass") final String inputClass);

    @GET
    @Path("/transformFunctions")
    @ApiOperation(value = "Gets available transform functions", response = String.class, responseContainer = "list")
    Set<Class> getTransformFunctions();

    @GET
    @Path("/elementGenerators")
    @ApiOperation(value = "Gets available element generators", response = String.class, responseContainer = "list")
    Set<Class> getElementGenerators();

    @GET
    @Path("/objectGenerators")
    @ApiOperation(value = "Gets available object generators", response = String.class, responseContainer = "list")
    Set<Class> getObjectGenerators();

    @GET
    @Path("/operations")
    @ApiOperation(value = "Gets all operations supported by the store.", response = String.class, responseContainer = "list")
    Set<Class> getOperations();

    @GET
    @Path("/storeTraits")
    @ApiOperation(value = "Gets all supported store traits", response = StoreTrait.class, responseContainer = "list")
    Set<StoreTrait> getStoreTraits();

    @GET
    @Path("/nextOperations/{className}")
    @ApiOperation(value = "Gets all the compatible operations that could be added to an operation chain after the provided operation.",
            response = String.class, responseContainer = "list")
    Set<Class> getNextOperations(@ApiParam(value = "an operation class name") @PathParam("className") final String operationClassName);

    @POST
    @Path("/isOperationSupported")
    @ApiOperation(value = "Determines whether the operation type supplied is supported by the store",
            response = Boolean.class)
    Boolean isOperationSupported(final Class operation);

    @GET
    @Path("/serialisedFields/{className}")
    @ApiOperation(value = "Gets all serialised fields for a given java class.", response = String.class, responseContainer = "list")
    Set<String> getSerialisedFields(@ApiParam(value = "a java class name") @PathParam("className") final String className);
}
