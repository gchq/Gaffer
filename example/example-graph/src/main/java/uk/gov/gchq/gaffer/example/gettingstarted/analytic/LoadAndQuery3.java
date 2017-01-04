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
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator3;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery3 extends LoadAndQuery {
    public LoadAndQuery3() {
        super("Filtering");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery3().run();
    }

    public CloseableIterable<Edge> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [generate] create some edges from the data file using our data generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final DataGenerator3 dataGenerator = new DataGenerator3();
        for (final String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator.getElement(s));
        }
        // ---------------------------------------------------------
        log("Elements generated from the data file.");
        for (final Element element : elements) {
            log("GENERATED_EDGES", element.toString());
        }
        log("");


        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.\n");


        log("\nAll edges containing the vertex 1. The counts have been aggregated\n");
        // [get simple] get all the edges that contain the vertex "1"
        // ---------------------------------------------------------
        final GetEdges<EntitySeed> getRelatedEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();
        final CloseableIterable<Edge> results = graph.execute(getRelatedEdges, user);
        // ---------------------------------------------------------
        for (final Element e : results) {
            log("GET_RELATED_EDGES_RESULT", e.toString());
        }


        // [get] rerun previous query with a filter to return only edges with a count more than 3
        // ---------------------------------------------------------
        final View view = new View.Builder()
                .edge("data", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan(3))
                                .build())
                        .build())
                .build();
        final GetEdges<EntitySeed> getRelatedEdgesWithCountMoreThan3 = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .view(view)
                .build();
        final CloseableIterable<Edge> filteredResults = graph.execute(getRelatedEdgesWithCountMoreThan3, user);
        // ---------------------------------------------------------
        log("\nAll edges containing the vertex 1 with an aggregated count more than than 3\n");
        for (final Element e : filteredResults) {
            log("GET_RELATED_ELEMENTS_WITH_COUNT_MORE_THAN_3_RESULT", e.toString());
        }

        return filteredResults;
    }
}
