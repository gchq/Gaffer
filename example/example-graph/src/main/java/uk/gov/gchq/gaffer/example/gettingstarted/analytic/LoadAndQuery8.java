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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator8;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.function.filter.IsLessThan;
import uk.gov.gchq.gaffer.function.filter.IsMoreThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.user.User;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadAndQuery8 extends LoadAndQuery {
    public static final Date JAN_01_16 = getDate("01/01/16");
    public static final Date JAN_02_16 = getDate("02/01/16");

    public static Date getDate(final String dateStr) {
        try {
            return new SimpleDateFormat("dd/MM/yy").parse(dateStr);
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Unable to parse date", e);
        }
    }

    public LoadAndQuery8() {
        super("Aggregation");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery8().run();
    }

    public Iterable<Edge> run() throws OperationException {
        // [user] Create a user who can see public and private data
        // ---------------------------------------------------------
        final User user = new User.Builder()
                .userId("user")
                .dataAuths("public", "private")
                .build();

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
                        .generator(new DataGenerator8())
                        .objects(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------


        // [get] Get all edges
        // ---------------------------------------------------------
        final GetAllEdges allEdgesOperation = new GetAllEdges();

        final Iterable<Edge> edges = graph.execute(allEdgesOperation, user);
        // ---------------------------------------------------------
        log("\nAll edges in daily time buckets:");
        for (final Edge edge : edges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get all edges summarised] Get all edges summarised (merge all time windows together)
        // This is achieved by overriding the 'groupBy' start and end time properties.
        // ---------------------------------------------------------
        final GetAllEdges edgesSummarisedOperation = new GetAllEdges.Builder()
                .view(new View.Builder()
                        .edge("data", new ViewElementDefinition.Builder()
                                .groupBy() // set the group by properties to 'none'
                                .build())
                        .build())
                .build();

        final Iterable<Edge> edgesSummarised = graph.execute(edgesSummarisedOperation, user);
        // ---------------------------------------------------------
        log("\nAll edges summarised:");
        for (final Edge edge : edgesSummarised) {
            log("GET_ALL_EDGES_SUMMARISED_RESULT", edge.toString());
        }


        // [get all edges summarised in time window] Get all edges summarised over a provided 2 day time period
        // This is achieved by overriding the 'groupBy' start and end time properties
        // and providing a filter.
        // ---------------------------------------------------------
        final GetAllEdges edgesSummarisedInTimeWindowOperation = new GetAllEdges.Builder()
                .view(new View.Builder()
                        .edge("data", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                                .select("startDate")
                                                .execute(new IsMoreThan(JAN_01_16, true))
                                                .select("endDate")
                                                .execute(new IsLessThan(JAN_02_16, true))
                                                .build()
                                )
                                .groupBy() // set the group by properties to 'none'
                                .build())
                        .build())
                .build();

        final Iterable<Edge> edgesSummarisedInTimeWindow = graph.execute(edgesSummarisedInTimeWindowOperation, user);
        // ---------------------------------------------------------
        log("\nEdges in 2 day time window:");
        for (final Edge edge : edgesSummarisedInTimeWindow) {
            log("GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT", edge.toString());
        }


        // Repeat with a public user and you will see the private edges are not part of the summarised edges
        final User publicUser = new User.Builder()
                .userId("public user")
                .dataAuths("public")
                .build();
        final Iterable<Edge> publicEdgesSummarisedInTimeWindow = graph.execute(edgesSummarisedInTimeWindowOperation, publicUser);
        log("\nPublic edges in 2 day time window:");
        for (final Edge edge : publicEdgesSummarisedInTimeWindow) {
            log("GET_PUBLIC_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT", edge.toString());
        }

        return publicEdgesSummarisedInTimeWindow;
    }
}
