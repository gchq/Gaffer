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

package uk.gov.gchq.gaffer.doc.user.walkthrough;

import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Filtering extends UserWalkthrough {
    public Filtering() {
        super("Filtering", "RoadAndRoadUse", RoadAndRoadUseElementGenerator.class);
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        // [generate] Create some edges from the simple data file using our Road Use generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final RoadAndRoadUseElementGenerator dataGenerator = new RoadAndRoadUseElementGenerator();
        for (final String line : IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUse/data.txt"))) {
            Iterables.addAll(elements, dataGenerator._apply(line));
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
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUse/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.");


        log("\nAll elements related to vertex 10. The counts have been aggregated\n");
        // [get simple] get all the edges that contain the vertex "10"
        // ---------------------------------------------------------
        final View view = new View.Builder()
                .edge("RoadUse")
                .build();
        final GetElements getRelatedElement = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .view(view)
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(getRelatedElement, user);
        // ---------------------------------------------------------
        for (final Element e : results) {
            log("GET_ELEMENTS_RESULT", e.toString());
        }


        // [get] rerun previous query with a filter to return only edges with a count more than 2
        // ---------------------------------------------------------
        final View viewWithFilter = new View.Builder()
                .edge("RoadUse", new ViewElementDefinition.Builder()
                        .postAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan(2L))
                                .build())
                        .build())
                .build();
        final GetElements getEdgesWithCountMoreThan2 = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .view(viewWithFilter)
                .build();
        final CloseableIterable<? extends Element> filteredResults = graph.execute(getEdgesWithCountMoreThan2, user);
        // ---------------------------------------------------------
        log("\nAll edges containing the vertex 10 with an aggregated count more than than 2\n");
        for (final Element e : filteredResults) {
            log("GET_ELEMENTS_WITH_COUNT_MORE_THAN_2_RESULT", e.toString());
        }

        return filteredResults;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Filtering walkthrough = new Filtering();
        walkthrough.run();
    }
}
