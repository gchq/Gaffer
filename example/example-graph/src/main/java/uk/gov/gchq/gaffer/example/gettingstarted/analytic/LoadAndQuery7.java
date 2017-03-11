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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.IsEdgeValidator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator7;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;

public class LoadAndQuery7 extends LoadAndQuery {
    public LoadAndQuery7() {
        super("Subgraphs");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery7().run();
    }

    public Iterable<Edge> run() throws OperationException {
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
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new DataGenerator7())
                        .objects(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------

        // Create some starting seeds for the sub graph.
        final Iterable<EntitySeed> seeds = Lists.newArrayList(new EntitySeed("1"));

        // Create a view to return only edges that have a count more than 1
        // Note we could have used a different view for each hop in order to
        // specify the edges we wish to hop down or to attempt to prevent caching
        // duplicate edges.
        final View view = new View.Builder()
                .edge("data", new ViewElementDefinition.Builder()
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
        final EntitySeedExtractor destVerticesExtractor = new EntitySeedExtractor(
                new IsEdgeValidator(),
                new AlwaysValid<>(),
                true,
                IdentifierType.DESTINATION);
        // ---------------------------------------------------------

        // [get] Create a sub graph
        // Start getting related edges with the given seeds.
        // Then update the export with the results
        // Between each hop we need to extract the destination vertices of the
        // previous edges.
        // Finally finish off by returning all the edges in the export.
        // ---------------------------------------------------------
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetEdges.Builder<EntitySeed>()
                        .seeds(seeds)
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(view)
                        .build())
                .then(new ExportToSet())
                .then(new GenerateObjects<Edge, EntitySeed>(destVerticesExtractor))
                .then(new GetEdges.Builder<EntitySeed>()
                        .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                        .view(view)
                        .build())
                .then(new ExportToSet())
                .then(new GetSetExport())
                .build();

        final Iterable<Edge> subGraph = (Iterable<Edge>) graph.execute(opChain, user);
        // ---------------------------------------------------------

        log("\nSub graph:");
        for (final Edge edge : subGraph) {
            log("SUB_GRAPH", edge.toString());
        }

        return subGraph;
    }
}
