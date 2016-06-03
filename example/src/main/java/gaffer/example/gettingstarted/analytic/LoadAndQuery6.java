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
package gaffer.example.gettingstarted.analytic;

import gaffer.data.element.Edge;
import gaffer.example.gettingstarted.generator.DataGenerator6;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import java.util.List;

public class LoadAndQuery6 extends LoadAndQuery {

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery6().run();
    }

    public Iterable<String> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/6/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/6/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        //create a graph using our schema and store properties
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        // Create data generator
        final DataGenerator6 dataGenerator = new DataGenerator6();

        // Load data into memory
        final List<String> data = DataUtils.loadData(getData());

        //add the edges to the graph using an operation chain consisting of:
        //generateElements - generating edges from the data (note these are directed edges)
        //addElements - add the edges to the graph
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(dataGenerator)
                        .objects(data)
                        .build())
                .then(new AddElements())
                .build();

        // Execute the add operation chain
        graph.execute(addOpChain, user);

        //create an operation chain consisting of 3 operations:
        //GetAdjacentEntitySeeds - starting at vertex 1 get all adjacent vertices (vertices at other end of outbound edges)
        //GetRelatedEdges - get outbound edges
        //GenerateObjects - convert the edges back into comma separated strings
        final OperationChain<Iterable<String>> opChain =
                new OperationChain.Builder()
                        .first(new GetAdjacentEntitySeeds.Builder()
                                .addSeed(new EntitySeed("1"))
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GetRelatedEdges.Builder<EntitySeed>()
                                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                                .build())
                        .then(new GenerateObjects.Builder<Edge, String>()
                                .generator(dataGenerator)
                                .build())
                        .build();

        // Execute the operation chain query
        final Iterable<String> results = graph.execute(opChain, user);
        log("\nFiltered edges converted back into comma separated strings. The counts have been aggregated\n");
        for (String result : results) {
            log(result);
        }

        return results;
    }
}
