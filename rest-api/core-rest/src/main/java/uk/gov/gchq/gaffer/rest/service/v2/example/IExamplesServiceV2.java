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

package uk.gov.gchq.gaffer.rest.service.v2.example;

import uk.gov.gchq.gaffer.operation.Operation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * An {@code IExamplesServiceV2} has methods to produce example {@link Operation}s to be applied
 * to the methods in {@link uk.gov.gchq.gaffer.rest.service.v2.IOperationServiceV2}.
 * Each example method path should be equal to the corresponding IOperationService method with /example as a prefix.
 * Each example method should return a populated {@link Operation} which can be used to call the
 * corresponding IOperationService method.
 * <p>
 * Ideally the example should work against any {@link uk.gov.gchq.gaffer.store.Store} with any set of schemas
 * and properties, however this may not be possible in all cases - but it should at least give a starting point for
 * constructing a valid {@link Operation}.
 */
@Path("/example")
@Produces(APPLICATION_JSON)
public interface IExamplesServiceV2 {

    @GET
    @Path("/graph/operations/execute")
    Operation execute() throws InstantiationException, IllegalAccessException;

    @GET
    @Path("/graph/operations/execute/chunked")
    Operation executeChunked() throws InstantiationException, IllegalAccessException;

}
