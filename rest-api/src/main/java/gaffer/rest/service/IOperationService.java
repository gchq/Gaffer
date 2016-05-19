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
import com.wordnik.swagger.annotations.ApiParam;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.OperationChain;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetEdgesBySeed;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetEntitiesBySeed;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.impl.get.GetRelatedEntities;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * An <code>IOperationService</code> has methods to execute {@link gaffer.operation.Operation}s on the
 * {@link gaffer.graph.Graph}.
 */
@Path("/graph/doOperation")
@Api(value = "/graph/doOperation", description = "Allows operations to be executed on the graph")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface IOperationService {

    @POST
    @ApiOperation(value = "Performs the given operation chain on the graph", response = Object.class)
    Object execute(final OperationChain operation);

    @POST
    @Path("/generate/objects")
    @ApiOperation(value = "Generate objects from elements", response = Object.class)
    Object generateObjects(final GenerateObjects operation);

    @POST
    @Path("/generate/elements")
    @ApiOperation(value = "Generate elements from objects", response = Element.class, responseContainer = "List")
    Iterable<Element> generateElements(final GenerateElements operation);

    @POST
    @Path("/get/elements/bySeed")
    @ApiOperation(value = "Gets elements by seed from the graph", response = Element.class, responseContainer = "List")
    Iterable<Element> getElementsBySeed(final GetElementsSeed<ElementSeed, Element> operation,
                                        @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/elements/related")
    @ApiOperation(value = "Gets related elements from the graph", response = Element.class, responseContainer = "List")
    Iterable<Element> getRelatedElements(final GetRelatedElements<ElementSeed, Element> operation,
                                         @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/entities/bySeed")
    @ApiOperation(value = "Gets entities by seed from the graph", response = Entity.class, responseContainer = "List")
    Iterable<Entity> getEntitiesBySeed(final GetEntitiesBySeed operation,
                                       @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/entities/related")
    @ApiOperation(value = "Gets related entities from the graph", response = Entity.class, responseContainer = "List")
    Iterable<Entity> getRelatedEntities(final GetRelatedEntities operation,
                                        @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/edges/bySeed")
    @ApiOperation(value = "Gets edge by seed from the graph", response = Edge.class, responseContainer = "List")
    Iterable<Edge> getEdgesBySeed(final GetEdgesBySeed operation,
                                  @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/edges/related")
    @ApiOperation(value = "Gets related edges from the graph", response = Edge.class, responseContainer = "List")
    Iterable<Edge> getRelatedEdges(final GetRelatedEdges operation,
                                   @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @POST
    @Path("/get/entitySeeds/adjacent")
    @ApiOperation(value = "Gets adjacent entity seeds", response = EntitySeed.class, responseContainer = "List")
    Iterable<EntitySeed> getAdjacentEntitySeeds(final GetAdjacentEntitySeeds operation,
                                                @ApiParam(value = "Number of results to return", required = false) @QueryParam("n") Integer n
    );

    @PUT
    @Path("/add/elements")
    @ApiOperation(value = "Add elements to the graph", response = Boolean.class)
    void addElements(final AddElements operation);
}
