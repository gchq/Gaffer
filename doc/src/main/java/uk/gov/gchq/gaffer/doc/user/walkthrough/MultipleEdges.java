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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultipleEdges extends UserWalkthrough {
    public MultipleEdges() {
        super("Multiple Edges", "RoadAndRoadUse", RoadAndRoadUseElementGenerator.class);
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


        // [add] Add the edges to the graph
        // ---------------------------------------------------------
        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.");


        // [get simple] Get all the edges related to vertex 10
        // ---------------------------------------------------------
        final GetElements getEdges = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .build();
        final CloseableIterable<? extends Element> edges = graph.execute(getEdges, user);
        // ---------------------------------------------------------
        log("\nAll edges containing vertex 10");
        log("\nNotice that the edges are aggregated within their groups");
        for (final Element e : edges) {
            log("GET_ELEMENTS_RESULT", e.toString());
        }


        // [get] Rerun the previous query with a View to specify which subset of results we want
        // ---------------------------------------------------------
        final View view = new View.Builder()
                .edge("RoadHasJunction")
                .build();
        final GetElements getRelatedRedEdges = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .view(view)
                .build();
        final CloseableIterable<? extends Element> redResults = graph.execute(getRelatedRedEdges, user);
        // ---------------------------------------------------------
        log("\nAll RoadHasJunction edges containing vertex 10\n");
        for (final Element e : redResults) {
            log("GET_ROAD_HAS_JUNCTION_EDGES_RESULT", e.toString());
        }

        return redResults;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final MultipleEdges walkthrough = new MultipleEdges();
        walkthrough.run();
    }
}
