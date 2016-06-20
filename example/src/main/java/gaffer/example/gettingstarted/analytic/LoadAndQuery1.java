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
import gaffer.data.element.Element;
import gaffer.example.gettingstarted.generator.DataGenerator1;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery1 extends LoadAndQuery {

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery1().run();
    }

    public Iterable<Edge> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/1/data.txt");
        setSchemaFolderLocation("/example/gettingstarted/1/schema");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        //create some edges from the data file using our data generator class
        final List<Element> elements = new ArrayList<>();
        final DataGenerator1 data1Generator = new DataGenerator1();
        log("Turn the data into Graph Edges\n");
        for (String s : DataUtils.loadData(getData())) {
            log(data1Generator.getElement(s).toString());
            elements.add(data1Generator.getElement(s));
        }
        log("");


        //create a graph using our schema and store properties
        final Graph graph1 = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();

        //add the edges to the graph
        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph1.execute(addElements, user);

        //get all the edges that contain the vertex "1"
        final GetRelatedEdges<EntitySeed> query = new GetRelatedEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        // Execute query
        final Iterable<Edge> results = graph1.execute(query, user);
        log("\nAll edges containing the vertex 1. The counts have been aggregated\n");
        for (Element e : results) {
            log(e.toString());
        }

        return results;
    }
}
