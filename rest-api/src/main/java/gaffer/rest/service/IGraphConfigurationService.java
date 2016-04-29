/*
 * Copyright 2016 Crown Copyright
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

package gaffer.rest.service;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import gaffer.operation.Operation;
import gaffer.store.schema.Schema;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * An <code>IGraphConfigurationService</code> has methods to get {@link gaffer.graph.Graph} configuration information
 * such as the {@link Schema} and available {@link gaffer.operation.Operation}s.
 */
@Path("/graph")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/graph", description = "Methods to get graph configuration information.")
public interface IGraphConfigurationService {
    @GET
    @Path("/schema")
    @ApiOperation(value = "Gets the schema", response = Class.class, responseContainer = "list")
    Schema getSchema();

    @GET
    @Path("/filterFunctions")
    @ApiOperation(value = "Gets available filter functions", response = Class.class, responseContainer = "list")
    List<Class> getFilterFunctions();

    @GET
    @Path("/transformFunctions")
    @ApiOperation(value = "Gets available transform functions", response = Class.class, responseContainer = "list")
    List<Class> getTransformFunctions();

    @GET
    @Path("/operations")
    @ApiOperation(value = "Gets available operations", response = Class.class, responseContainer = "list")
    List<Class> getOperations();

    @GET
    @Path("/generators")
    @ApiOperation(value = "Gets available generators", response = Class.class, responseContainer = "list")
    List<Class> getGenerators();

    @GET
    @Path("/supportedOperations")
    @ApiOperation(value = "Gets all operations supported by the store", response = Class.class, responseContainer = "list")
    List<Class<? extends Operation>> getSupportedOperations();

    @POST
    @Path("/isOperationSupported")
    @ApiOperation(value = "Determines whether the operation type supplied is supported by the store",
            response = Boolean.class)
    Boolean isOperationSupported(final Class<? extends Operation> operation);
}
