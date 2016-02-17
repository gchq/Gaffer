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

import gaffer.data.element.Element;
import gaffer.operation.OperationChain;
import gaffer.operation.data.ElementSeed;
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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * An <code>IExamplesService</code> has methods to produce example {@link gaffer.operation.Operation}s to be applied
 * to the methods in {@link gaffer.rest.service.IOperationService}.
 * Each example method path should be equal to the corresponding IOperationService method with /example as a prefix.
 * Each example method should return a populated {@link gaffer.operation.Operation} which can be used to call the
 * corresponding IOperationService method.
 * <p>
 * Ideally the example should work against any {@link gaffer.store.Store} with any set of schemas
 * and properties, however this may not be possible in all cases - but it should at least give a starting point for
 * constructing a valid {@link gaffer.operation.Operation}.
 */
@Path("/example")
@Produces(MediaType.APPLICATION_JSON)
public interface IExamplesService {
    @GET
    @Path("/graph/execute")
    OperationChain execute();

    @GET
    @Path("/graph/doOperation/get/elements/bySeed")
    GetElementsSeed<ElementSeed, Element> getElementsBySeed();

    @GET
    @Path("/graph/doOperation/get/elements/related")
    GetRelatedElements<ElementSeed, Element> getRelatedElements();

    @GET
    @Path("/graph/doOperation/get/entities/bySeed")
    GetEntitiesBySeed getEntitiesBySeed();

    @GET
    @Path("/graph/doOperation/get/entities/related")
    GetRelatedEntities getRelatedEntities();

    @GET
    @Path("/graph/doOperation/get/edges/bySeed")
    GetEdgesBySeed getEdgesBySeed();

    @GET
    @Path("/graph/doOperation/get/edges/related")
    GetRelatedEdges getRelatedEdges();

    @GET
    @Path("/graph/doOperation/get/entitySeeds/adjacent")
    GetAdjacentEntitySeeds adjacentEntitySeeds();

    @GET
    @Path("/graph/doOperation/add/elements")
    AddElements addElements();

    @GET
    @Path("/graph/doOperation/generate/objects")
    GenerateObjects generateObjects();

    @GET
    @Path("/graph/doOperation/generate/elements")
    GenerateElements generateElements();
}
