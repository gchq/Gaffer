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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import uk.gov.gchq.gaffer.doc.dev.generator.RoadAndRoadUseElementGenerator;
import uk.gov.gchq.gaffer.doc.dev.generator.RoadUseCsvGenerator;
import uk.gov.gchq.gaffer.doc.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

public class OperationChains extends DevWalkthrough {
    public OperationChains() {
        super("Operation Chains", "RoadUse/data.txt", "RoadAndRoadUse/schema", RoadAndRoadUseElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new OperationChains().run();
    }

    public Iterable<? extends String> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------

        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] Create a data generator and add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseElementGenerator())
                        .input(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------


        // [get] Create and execute an operation chain consisting of 3 operations:
        //GetAdjacentIds - starting at vertex 1 get all adjacent vertices (vertices at other end of outbound edges)
        //GetRelatedEdges - get outbound edges
        //GenerateObjects - convert the edges back into comma separated strings
        // ---------------------------------------------------------
        final OperationChain<Iterable<? extends String>> opChain =
                new OperationChain.Builder()
                        .first(new GetAdjacentIds.Builder()
                                .input(new EntitySeed("M5"))
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GetElements.Builder()
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GenerateObjects.Builder<String>()
                                .generator(new RoadUseCsvGenerator())
                                .build())
                        .build();

        final Iterable<? extends String> results = graph.execute(opChain, user);
        // ---------------------------------------------------------

        log("\nFiltered edges converted back into comma separated strings. The counts have been aggregated\n");
        for (final String result : results) {
            log("RESULT", result);
        }

        return results;
    }
}
