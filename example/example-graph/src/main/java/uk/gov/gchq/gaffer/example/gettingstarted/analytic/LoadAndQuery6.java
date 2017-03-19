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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator6;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.util.List;

public class LoadAndQuery6 extends LoadAndQuery {
    public LoadAndQuery6() {
        super("Operation Chains");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery6().run();
    }

    public CloseableIterable<String> run() throws OperationException {
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
        final DataGenerator6 dataGenerator = new DataGenerator6();
        final List<String> data = DataUtils.loadData(getData());

        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(dataGenerator)
                        .objects(data)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------


        // [get] Create and execute an operation chain consisting of 3 operations:
        //GetAdjacentEntitySeeds - starting at vertex 1 get all adjacent vertices (vertices at other end of outbound edges)
        //GetRelatedEdges - get outbound edges
        //GenerateObjects - convert the edges back into comma separated strings
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<String>> opChain =
                new OperationChain.Builder()
                        .first(new GetAdjacentEntitySeeds.Builder()
                                .addSeed(new EntitySeed("1"))
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GetEdges.Builder<EntitySeed>()
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GenerateObjects.Builder<Edge, String>()
                                .generator(dataGenerator)
                                .build())
                        .build();

        final CloseableIterable<String> results = graph.execute(opChain, user);
        // ---------------------------------------------------------

        log("\nFiltered edges converted back into comma separated strings. The counts have been aggregated\n");
        for (final String result : results) {
            log("RESULT", result);
        }

        return results;
    }
}
