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
import org.glassfish.jersey.server.ChunkedOutput;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * An {@code IOperationService} has methods to execute {@link uk.gov.gchq.gaffer.operation.Operation}s on the
 * {@link uk.gov.gchq.gaffer.graph.Graph}.
 */
@Path("/graph/doOperation")
@Api(value = "operations")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public interface IOperationService {

    @POST
    @ApiOperation(value = "Performs the given operation chain on the graph", response = Object.class)
    Object execute(final OperationChainDAO opChain);

    @POST
    @Path("/operation")
    @ApiOperation(value = "Performs the given operation on the graph", response = Object.class)
    Object execute(final Operation operation);

    @POST
    @Path("/chunked/operation")
    @ApiOperation(value = "Performs the given operation on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class)
    ChunkedOutput<String> executeChunked(final Operation operation);

    @POST
    @Path("/chunked")
    @ApiOperation(value = "Performs the given operation chain on the graph, returned chunked output. NOTE - does not work in Swagger.", response = Object.class)
    ChunkedOutput<String> executeChunkedChain(final OperationChainDAO<CloseableIterable<Element>> opChain);

    @POST
    @Path("/generate/objects")
    @ApiOperation(value = "Generate objects from elements", response = Object.class, responseContainer = "List")
    CloseableIterable<Object> generateObjects(final GenerateObjects<Object> operation);

    @POST
    @Path("/generate/elements")
    @ApiOperation(value = "Generate elements from objects", response = Element.class, responseContainer = "List")
    CloseableIterable<Element> generateElements(final GenerateElements<Object> operation);

    @POST
    @Path("/get/entityIds/adjacent")
    @ApiOperation(value = "Gets adjacent entity seeds", response = EntityId.class, responseContainer = "List")
    CloseableIterable<EntityId> getAdjacentIds(final GetAdjacentIds operation);

    @POST
    @Path("/get/elements/all")
    @ApiOperation(value = "Gets all elements", response = Element.class, responseContainer = "List")
    CloseableIterable<Element> getAllElements(final GetAllElements operation);

    @POST
    @Path("/get/elements")
    @ApiOperation(value = "Gets elements", response = Element.class, responseContainer = "List")
    CloseableIterable<Element> getElements(final GetElements operation);

    @PUT
    @Path("/add/elements")
    @ApiOperation(value = "Add elements to the graph", response = Boolean.class)
    void addElements(final AddElements operation);
}
