/*
 * Copyright 2016-2017 Crown Copyright
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

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import java.io.IOException;

public class Subgraphs extends UserWalkthrough {
    public Subgraphs() {
        super("Subgraphs", "RoadAndRoadUseWithTimesAndCardinalities", RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator.class);
    }

    @Override
    public Iterable<? extends Element> run() throws OperationException, IOException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [add] Create a data generator and add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator())
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/data.txt")))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("The elements have been added.");


        // [get] Create a sub graph
        // Start getting related edges with the given seeds.
        // Then update the export with the results
        // Between each hop we need to extract the destination vertices of the
        // previous edges and convert them to EntitySeeds.
        // Finally finish off by returning all the edges in the export.
        // ---------------------------------------------------------
        final OperationChain<Iterable<?>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("M5"))
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("RoadHasJunction")
                                .build())
                        .build())
                .then(new ExportToSet<>())
                .then(new ToVertices.Builder()
                        .edgeVertices(ToVertices.EdgeVertices.DESTINATION)
                        .build())
                .then(new ToEntitySeeds())
                .then(new GetElements.Builder()
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("RoadUse", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("count")
                                                .execute(new IsMoreThan(1L))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new ExportToSet<>())
                .then(new DiscardOutput())
                .then(new GetSetExport())
                .build();
        // ---------------------------------------------------------

        final Iterable<? extends Element> subGraph = (Iterable<? extends Element>) graph.execute(opChain, user);
        log("\nSub graph:");
        for (final Element edge : subGraph) {
            log("SUB_GRAPH", edge.toString());
        }

        return subGraph;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Subgraphs walkthrough = new Subgraphs();
        walkthrough.run();
    }
}
