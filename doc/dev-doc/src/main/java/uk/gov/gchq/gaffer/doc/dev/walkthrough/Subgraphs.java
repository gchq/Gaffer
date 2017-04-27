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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.dev.generator.RoadAndRoadUseElementGenerator;
import uk.gov.gchq.gaffer.doc.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

public class Subgraphs extends DevWalkthrough {
    public Subgraphs() {
        super("Subgraphs", "RoadUse/data.txt", "RoadAndRoadUse/schema", RoadAndRoadUseElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new Subgraphs().run();
    }

    public Iterable<? extends Element> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseElementGenerator())
                        .input(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------

        // Create a view to return only edges that have a count more than 1
        // Note we could have used a different view for each hop in order to
        // specify the edges we wish to hop down or to attempt to prevent caching
        // duplicate edges.
        final View view = new View.Builder()
                .edge("RoadUse", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan(1))
                                .build())
                        .build())
                .build();

        // [extractor] Create a generator that will extract entity seeds
        // This generator will extract just the destination vertices from edges
        // and skip any entities.
        // ---------------------------------------------------------
        final EntityIdExtractor destVerticesExtractor = new EntityIdExtractor(IdentifierType.DESTINATION);
        // ---------------------------------------------------------

        // [get] Create a sub graph
        // Start getting related edges with the given seeds.
        // Then update the export with the results
        // Between each hop we need to extract the destination vertices of the
        // previous edges.
        // Finally finish off by returning all the edges in the export.
        // ---------------------------------------------------------
        final OperationChain<Iterable<?>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("M5"))
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .build())
                .then(new ExportToSet<>())
                .then(new GenerateObjects<>(destVerticesExtractor))
                .then(new GetElements.Builder()
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(new View.Builder()
                                .edge("RoadUse", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("count")
                                                .execute(new IsMoreThan(1))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new ExportToSet<>())
                .then(new DiscardOutput())
                .then(new GetSetExport())
                .build();

        final Iterable<? extends Element> subGraph = (Iterable<? extends Element>) graph.execute(opChain, user);
        // ---------------------------------------------------------

        log("\nSub graph:");
        for (final Element edge : subGraph) {
            log("SUB_GRAPH", edge.toString());
        }

        return subGraph;
    }
}
