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

package uk.gov.gchq.gaffer.rest.service;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * An <code>IExamplesService</code> has methods to produce example {@link uk.gov.gchq.gaffer.operation.Operation}s to be applied
 * to the methods in {@link uk.gov.gchq.gaffer.rest.service.IOperationService}.
 * Each example method path should be equal to the corresponding IOperationService method with /example as a prefix.
 * Each example method should return a populated {@link uk.gov.gchq.gaffer.operation.Operation} which can be used to call the
 * corresponding IOperationService method.
 * <p>
 * Ideally the example should work against any {@link uk.gov.gchq.gaffer.store.Store} with any set of schemas
 * and properties, however this may not be possible in all cases - but it should at least give a starting point for
 * constructing a valid {@link uk.gov.gchq.gaffer.operation.Operation}.
 */
@Path("/example")
@Produces(MediaType.APPLICATION_JSON)
public interface IExamplesService {
    @GET
    @Path("/graph/doOperation")
    OperationChain execute();

    @GET
    @Path("/graph/doOperation/chunked")
    OperationChain executeChunked();

    @GET
    @Path("/graph/doOperation/get/elements/bySeed")
    GetElements<ElementSeed, Element> getElementsBySeed();

    @GET
    @Path("/graph/doOperation/get/elements/related")
    GetElements<ElementSeed, Element> getRelatedElements();

    @GET
    @Path("/graph/doOperation/get/entities/bySeed")
    GetEntities<ElementSeed> getEntitiesBySeed();

    @GET
    @Path("/graph/doOperation/get/entities/related")
    GetEntities<ElementSeed> getRelatedEntities();

    @GET
    @Path("/graph/doOperation/get/edges/bySeed")
    GetEdges<ElementSeed> getEdgesBySeed();

    @GET
    @Path("/graph/doOperation/get/edges/related")
    GetEdges<ElementSeed> getRelatedEdges();

    @GET
    @Path("/graph/doOperation/get/entitySeeds/adjacent")
    GetAdjacentEntitySeeds getAdjacentEntitySeeds();

    @GET
    @Path("/graph/doOperation/get/elements/all")
    GetAllElements getAllElements();

    @GET
    @Path("/graph/doOperation/get/entities/all")
    GetAllEntities getAllEntities();

    @GET
    @Path("/graph/doOperation/get/edges/all")
    GetAllEdges getAllEdges();

    @GET
    @Path("/graph/doOperation/get/elements")
    GetElements getElements();

    @GET
    @Path("/graph/doOperation/get/entities")
    GetEntities getEntities();

    @GET
    @Path("/graph/doOperation/get/edges")
    GetEdges getEdges();

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
