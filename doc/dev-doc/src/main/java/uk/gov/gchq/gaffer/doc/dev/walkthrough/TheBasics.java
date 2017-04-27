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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.dev.generator.RoadUseElementGenerator;
import uk.gov.gchq.gaffer.doc.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class TheBasics extends DevWalkthrough {
    public TheBasics() {
        super("The basics", "RoadUse/data.txt", "RoadUse/schema", RoadUseElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new TheBasics().run();
    }

    public CloseableIterable<? extends Element> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [generate] Create some edges from the simple data file using our Road Use generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final RoadUseElementGenerator dataGenerator = new RoadUseElementGenerator();
        for (final String line : DataUtils.loadData(getData())) {
            elements.add(dataGenerator._apply(line));
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
                .input(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.\n");


        // [get] Get all the edges that contain the vertex "10"
        // ---------------------------------------------------------
        final GetElements query = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .view(new View.Builder()
                        .edge("RoadUse")
                        .build())
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(query, user);
        // ---------------------------------------------------------
        log("All edges containing the vertex 10. The counts have been aggregated.");
        for (final Element e : results) {
            log("GET_RELATED_EDGES_RESULT", e.toString());
        }

        return results;
    }
}
