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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Cardinalities extends UserWalkthrough {
    public Cardinalities() {
        super("Cardinalities", "RoadAndRoadUseWithTimesAndCardinalities", RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator.class);
    }

    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        // [generate] Create some edges from the simple data file using our Road Use generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator dataGenerator = new RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator();
        for (final String line : IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/data.txt"))) {
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
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
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


        // [get] Get all edges
        // ---------------------------------------------------------
        final CloseableIterable<? extends Element> edges = graph.execute(new GetAllElements(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Element edge : edges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get all cardinalities] Get all cardinalities
        // ---------------------------------------------------------
        final GetAllElements getAllCardinalities =
                new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("Cardinality")
                                .build())
                        .build();
        // ---------------------------------------------------------
        final CloseableIterable<? extends Element> allCardinalities = graph.execute(getAllCardinalities, user);
        log("\nAll cardinalities");
        for (final Element cardinality : allCardinalities) {
            final String edgeGroup = cardinality.getProperty("edgeGroup").toString();
            log("ALL_CARDINALITIES_RESULT", "Vertex " + ((Entity) cardinality).getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) cardinality.getProperty("hllp")).cardinality());
        }

        // [get all summarised cardinalities] Get all summarised cardinalities over all edges
        // ---------------------------------------------------------
        final GetAllElements getAllSummarisedCardinalities =
                new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity("Cardinality", new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .build())
                        .build();
        // ---------------------------------------------------------
        final CloseableIterable<? extends Element> allSummarisedCardinalities = graph.execute(getAllSummarisedCardinalities, user);
        log("\nAll summarised cardinalities");
        for (final Element cardinality : allSummarisedCardinalities) {
            final String edgeGroup = cardinality.getProperty("edgeGroup").toString();
            log("ALL_SUMMARISED_CARDINALITIES_RESULT", "Vertex " + ((Entity) cardinality).getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) cardinality.getProperty("hllp")).cardinality());
        }

        // [get roaduse edge cardinality 10] Get the cardinality value at vertex 10 for RoadUse edges
        // ---------------------------------------------------------
        final GetElements getCardinalities =
                new GetElements.Builder()
                        .input(new EntitySeed("10"))
                        .view(new View.Builder()
                                .entity("Cardinality", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("edgeGroup")
                                                .execute(new IsEqual(CollectionUtil.treeSet("RoadUse")))
                                                .build())
                                        .build())
                                .build())
                        .build();
        // ---------------------------------------------------------

        final Element roadUse10Cardinality = graph.execute(getCardinalities, user).iterator().next();
        log("\nRed edge cardinality at vertex 10:");
        final String edgeGroup = (roadUse10Cardinality.getProperty("edgeGroup")).toString();
        log("CARDINALITY_OF_10_RESULT", "Vertex " + ((Entity) roadUse10Cardinality).getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) roadUse10Cardinality.getProperty("hllp")).cardinality());

        return allSummarisedCardinalities;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Cardinalities walkthrough = new Cardinalities();
        walkthrough.run();
    }
}
