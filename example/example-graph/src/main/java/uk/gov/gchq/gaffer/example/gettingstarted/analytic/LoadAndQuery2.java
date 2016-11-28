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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator2;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery2 extends LoadAndQuery {
    public LoadAndQuery2() {
        super("Querying for specific Groups");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery2().run();
    }

    public CloseableIterable<Edge> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------

        // [generate] Create some edges from the data file using our data generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final DataGenerator2 dataGenerator = new DataGenerator2();
        for (final String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator.getElement(s));
        }
        // ---------------------------------------------------------
        log("Elements generated from the data file.");
        for (final Element element : elements) {
            log("GENERATED_EDGES", element.toString());
        }
        log("");


        // [graph] Create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] Add the edges to the graph
        // ---------------------------------------------------------
        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.\n");


        // [get simple] Get all the edges related to vertex 1
        // ---------------------------------------------------------
        final GetEdges<EntitySeed> getRelatedEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();
        final CloseableIterable<Edge> allColoursResults = graph.execute(getRelatedEdges, user);
        // ---------------------------------------------------------
        log("\nAll edges containing vertex 1");
        log("\nNotice that the edges are aggregated within their groups");
        for (final Element e : allColoursResults) {
            log("GET_RELATED_EDGES_RESULT", e.toString());
        }


        // [get] Rerun the previous query with a View to specify which subset of results we want
        // ---------------------------------------------------------
        final View view = new View.Builder()
                .edge("red")
                .build();
        final GetEdges<EntitySeed> getRelatedRedEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .view(view)
                .build();
        final CloseableIterable<Edge> redResults = graph.execute(getRelatedRedEdges, user);
        // ---------------------------------------------------------
        log("\nAll red edges containing vertex 1\n");
        for (final Element e : redResults) {
            log("GET_RELATED_RED_EDGES_RESULT", e.toString());
        }

        return redResults;
    }
}
