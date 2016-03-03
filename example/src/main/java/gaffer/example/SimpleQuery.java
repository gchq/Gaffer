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

package gaffer.example;

import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;
import gaffer.example.data.Certificate;
import gaffer.example.data.SampleData;
import gaffer.example.data.Viewing;
import gaffer.example.data.schema.Group;
import gaffer.example.generator.DataGenerator;
import gaffer.example.generator.ViewingGenerator;
import gaffer.graph.Graph;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetRelatedEdges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example shows how to interact with a Gaffer graph with a simple and complex query.
 */
public class SimpleQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleQuery.class);

    /**
     * The authorisation for the user doing the query.
     * Here we are setting the authorisation to include all certificates so the user will be able to see all the data.
     * This only applies to the Accumulo Store.
     */
    private static final String AUTH = Certificate.U.name() + ","
            + Certificate.PG.name() + ","
            + Certificate._12A.name() + ","
            + Certificate._15.name() + ","
            + Certificate._18.name();

    public static void main(final String[] args) throws OperationException {
        final Iterable<Viewing> simpleResults = new SimpleQuery().run();
        final StringBuilder builder = new StringBuilder("Results from simple query:\n");
        for (Object obj : simpleResults) {
            builder.append(obj).append("\n");
        }
        LOGGER.info(builder.toString());
    }


    /**
     * Finds all viewings of filmA
     * <ul>
     * <li>Starts from a seed of filmA.</li>
     * <li>Finds all viewings (edges) with related to filmA (source or destination)</li>
     * <li>Then generates Viewing domain objects from the edges</li>
     * <li>Then returns the Viewing domain objects</li>
     * </ul>
     * This query can be written in JSON and executed over a rest service - see resources/simpleQuery.json and
     * resources/addData.json
     *
     * @return the viewing domain objects
     * @throws OperationException if operation chain fails to be executed on the graph
     */
    public Iterable<Viewing> run() throws OperationException {
        // Create Graph
        final Graph graph = new Graph(SimpleQuery.class.getResourceAsStream("/dataSchema.json"),
                SimpleQuery.class.getResourceAsStream("/storeSchema.json"),
                SimpleQuery.class.getResourceAsStream("/store.properties"));

        // Populate the graph with some example data
        // Create an operation chain. The output from the first operation is passed in as the input the second operation.
        // So the chain operation will generate elements from the domain objects then add these elements to the graph.
        final OperationChain<Void> populateChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<>()
                        .objects(new SampleData().generate())
                        .generator(new DataGenerator())
                        .build())
                .then(new AddElements.Builder()
                        .build())
                .build();

        // Execute the operation chain on the graph
        graph.execute(populateChain);


        // Create an operation chain.
        // So the chain operation will get related edges then generate domain objects from the edges.
        final OperationChain<Iterable<Viewing>> queryChain = new OperationChain.Builder()
                .first(new GetRelatedEdges.Builder()
                        .view(new View.Builder()
                                .edge(Group.VIEWING)
                                .build())
                        .addSeed(new EntitySeed("filmA"))
                        .option(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTH)
                        .build())
                .then(new GenerateObjects.Builder<Edge, Viewing>()
                        .generator(new ViewingGenerator())
                        .option(AccumuloStoreConstants.OPERATION_AUTHORISATIONS, AUTH)
                        .build())
                .build();

        // Execute the operation on the graph
        return graph.execute(queryChain);
    }
}
