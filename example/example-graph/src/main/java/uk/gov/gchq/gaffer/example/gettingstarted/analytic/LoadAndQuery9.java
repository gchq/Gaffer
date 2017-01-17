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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator9;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.function.filter.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.user.User;

public class LoadAndQuery9 extends LoadAndQuery {
    public LoadAndQuery9() {
        super("Cardinalities");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery9().run();
    }

    public Iterable<Entity> run() throws OperationException {
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


        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new DataGenerator9())
                        .objects(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------


        // [get] Get all edges
        // ---------------------------------------------------------
        final Iterable<Edge> edges = graph.execute(new GetAllEdges(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Edge edge : edges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get all cardinalities] Get all cardinalities
        // ---------------------------------------------------------
        final GetAllEntities getAllCardinalities =
                new GetAllEntities.Builder()
                        .view(new View.Builder()
                                .entity("Cardinality")
                                .build())
                        .build();
        // ---------------------------------------------------------
        final CloseableIterable<Entity> allCardinalities = graph.execute(getAllCardinalities, user);
        log("\nAll cardinalities");
        for (final Entity cardinality : allCardinalities) {
            final String edgeGroup = (cardinality.getProperty("edgeGroup")).toString();
            log("ALL_CARDINALITIES_RESULT", "Vertex " + cardinality.getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) cardinality.getProperty("hllp")).cardinality());
        }

        // [get all summarised cardinalities] Get all summarised cardinalities over all edges
        // ---------------------------------------------------------
        final GetAllEntities getAllSummarisedCardinalities =
                new GetAllEntities.Builder()
                        .view(new View.Builder()
                                .entity("Cardinality", new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .build())
                        .build();
        // ---------------------------------------------------------
        final CloseableIterable<Entity> allSummarisedCardinalities = graph.execute(getAllSummarisedCardinalities, user);
        log("\nAll summarised cardinalities");
        for (final Entity cardinality : allSummarisedCardinalities) {
            final String edgeGroup = (cardinality.getProperty("edgeGroup")).toString();
            log("ALL_SUMMARISED_CARDINALITIES_RESULT", "Vertex " + cardinality.getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) cardinality.getProperty("hllp")).cardinality());
        }

        // [get red edge cardinality 1] Get the cardinality value at vertex 1 for red edges
        // ---------------------------------------------------------
        final GetEntities<EntitySeed> getCardinalities =
                new GetEntities.Builder<EntitySeed>()
                        .addSeed(new EntitySeed("1"))
                        .view(new View.Builder()
                                .entity("Cardinality", new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select("edgeGroup")
                                                .execute(new IsEqual(CollectionUtil.treeSet("red")))
                                                .build())
                                        .build())
                                .build())
                        .build();
        // ---------------------------------------------------------

        final Entity redCardinality = graph.execute(getCardinalities, user).iterator().next();
        // ---------------------------------------------------------
        log("\nRed edge cardinality at vertex 1:");
        final String edgeGroup = (redCardinality.getProperty("edgeGroup")).toString();
        log("CARDINALITY_OF_1_RESULT", "Vertex " + redCardinality.getVertex() + " " + edgeGroup + ": " + ((HyperLogLogPlus) redCardinality.getProperty("hllp")).cardinality());

        return allSummarisedCardinalities;
    }
}
