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
import gaffer.data.elementdefinition.view.View;
import gaffer.example.gettingstarted.generator.DataGenerator2;
import gaffer.example.gettingstarted.util.DataUtils;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery2 extends LoadAndQuery {
    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery2().run();
    }

    public Iterable<Edge> run() throws OperationException {
        final User user = new User("user01");

        setDataFileLocation("/example/gettingstarted/2/data.txt");
        setDataSchemaLocation("/example/gettingstarted/2/schema/dataSchema.json");
        setDataTypesLocation("/example/gettingstarted/2/schema/dataTypes.json");
        setStoreTypesLocation("/example/gettingstarted/2/schema/storeTypes.json");
        setStorePropertiesLocation("/example/gettingstarted/mockaccumulostore.properties");

        final Graph graph2 = new Graph.Builder()
                .addSchema(getDataSchema())
                .addSchema(getDataTypes())
                .addSchema(getStoreTypes())
                .storeProperties(getStoreProperties())
                .build();

        final List<Element> elements = new ArrayList<>();
        final DataGenerator2 dataGenerator2 = new DataGenerator2();
        System.out.println("\nTurn the data into Graph Edges\n");
        for (String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator2.getElement(s));
            System.out.println(dataGenerator2.getElement(s).toString());
        }
        System.out.println("");

        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();

        graph2.execute(addElements, user);

        //get all the edges
        final GetRelatedEdges getRelatedEdges = new GetRelatedEdges.Builder()
                .addSeed(new EntitySeed("1"))
                .build();

        System.out.println("\nAll edges containing vertex 1");
        System.out.println("\nNotice that the edges are aggregated within their groups");
        final Iterable<Edge> allColoursResults = graph2.execute(getRelatedEdges, user);
        for (Element e : allColoursResults) {
            System.out.println(e.toString());
        }

        //create a View to specify which subset of results we want
        final View view = new View.Builder()
                .edge("red")
                .build();
        //rerun the previous query with our view
        getRelatedEdges.setView(view);

        System.out.println("\nAll red edges containing vertex 1\n");
        final Iterable<Edge> redResults = graph2.execute(getRelatedEdges, user);
        for (Element e : redResults) {
            System.out.println(e.toString());
        }

        return redResults;
    }
}
