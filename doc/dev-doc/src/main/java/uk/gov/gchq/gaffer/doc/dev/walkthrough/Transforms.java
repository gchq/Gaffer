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

import com.google.common.collect.Iterables;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.dev.function.transform.DescriptionTransform;
import uk.gov.gchq.gaffer.doc.dev.generator.RoadAndRoadUseElementGenerator;
import uk.gov.gchq.gaffer.doc.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class Transforms extends DevWalkthrough {
    public Transforms() {
        super("Transforms", "RoadAndRoadUse", RoadAndRoadUseElementGenerator.class);
    }

    public CloseableIterable<? extends Element> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [generate] create some edges from the data file using our data generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final RoadAndRoadUseElementGenerator dataGenerator = new RoadAndRoadUseElementGenerator();
        for (final String s : DataUtils.loadData(getData())) {
            Iterables.addAll(elements, dataGenerator._apply(s));
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
        final AddElements addElements = new AddElements.Builder()
                .input(elements)
                .build();
        graph.execute(addElements, user);
        // ---------------------------------------------------------
        log("The elements have been added.\n");


        // [get simple] get all the edges that contain the vertex "1"
        // ---------------------------------------------------------
        final GetElements getRelatedEdges = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .build();
        final CloseableIterable<? extends Element> results = graph.execute(getRelatedEdges, user);
        // ---------------------------------------------------------
        log("\nAll edges containing the vertex 10. The counts and 'things' have been aggregated\n");
        for (final Element e : results) {
            log("GET_ELEMENTS_RESULT", e.toString());
        }


        // [transform] Create a description transient property using an element transformer
        // ---------------------------------------------------------
        final ElementTransformer descriptionTransformer = new ElementTransformer.Builder()
                .select("SOURCE", "DESTINATION", "count")
                .execute(new DescriptionTransform())
                .project("description")
                .build();
        // ---------------------------------------------------------


        // [get] Add the element transformer to the view and run the query
        // ---------------------------------------------------------
        final View view = new View.Builder()
                .edge("RoadUse", new ViewElementDefinition.Builder()
                        .transientProperty("description", String.class)
                        .transformer(descriptionTransformer)
                        .build())
                .build();
        final GetElements getRelatedEdgesWithDescription = new GetElements.Builder()
                .input(new EntitySeed("10"))
                .view(view)
                .build();
        final CloseableIterable<? extends Element> resultsWithDescription = graph.execute(getRelatedEdgesWithDescription, user);
        // ---------------------------------------------------------
        log("\nWe can add a new property to the edges that is calculated from the aggregated values of other properties\n");
        for (final Element e : resultsWithDescription) {
            log("GET_ELEMENTS_WITH_DESCRIPTION_RESULT", e.toString());
        }

        return resultsWithDescription;
    }

    public static void main(final String[] args) throws OperationException {
        final DevWalkthrough walkthrough = new Transforms();
        walkthrough.log(walkthrough.walkthrough());
    }
}
