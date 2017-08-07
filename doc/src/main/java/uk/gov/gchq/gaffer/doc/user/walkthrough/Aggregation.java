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

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Min;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Aggregation extends UserWalkthrough {
    public static final Date MAY_01_2000 = getDate("2000-05-01");
    public static final Date MAY_02_2000 = getDate("2000-05-02");
    public static final Date MAY_03_2000 = getDate("2000-05-03");

    public Aggregation() {
        super("Aggregation", "RoadAndRoadUseWithTimes", RoadAndRoadUseWithTimesElementGenerator.class);
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .graphId("graph1")
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimes/schema"))
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
                        .generator(new RoadAndRoadUseWithTimesElementGenerator())
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimes/data.txt")))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("The elements have been added.");


        // [get] Get all RoadUse edges
        // ---------------------------------------------------------
        final GetAllElements getAllRoadUseEdges = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge("RoadUse")
                        .build())
                .build();

        final CloseableIterable<? extends Element> roadUseElements = graph.execute(getAllRoadUseEdges, user);
        // ---------------------------------------------------------
        log("\nAll RoadUse edges in daily time buckets:");
        for (final Element element : roadUseElements) {
            log("GET_ALL_EDGES_RESULT", element.toString());
        }


        // [get all edges summarised] Get all edges summarised (merge all time windows together)
        // This is achieved by overriding the 'groupBy' start and end time properties.
        // ---------------------------------------------------------
        final GetAllElements edgesSummarisedOperation = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge("RoadUse", new ViewElementDefinition.Builder()
                                .groupBy() // set the group by properties to 'none'
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> edgesSummarised = graph.execute(edgesSummarisedOperation, user);
        // ---------------------------------------------------------
        log("\nAll edges summarised:");
        for (final Element edge : edgesSummarised) {
            log("GET_ALL_EDGES_SUMMARISED_RESULT", edge.toString());
        }


        // [get all edges summarised in time window] Get all edges summarised over a provided 2 day time period
        // This is achieved by overriding the 'groupBy' start and end time properties
        // and providing a filter.
        // ---------------------------------------------------------
        final GetAllElements edgesSummarisedInTimeWindowOperation = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge("RoadUse", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("startDate")
                                        .execute(new IsMoreThan(MAY_01_2000, true))
                                        .select("endDate")
                                        .execute(new IsLessThan(MAY_02_2000, false))
                                        .build()
                                )
                                .groupBy() // set the group by properties to 'none'
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> edgesSummarisedInTimeWindow = graph.execute(edgesSummarisedInTimeWindowOperation, user);
        // ---------------------------------------------------------
        log("\nEdges in 2 day time window:");
        for (final Element edge : edgesSummarisedInTimeWindow) {
            log("GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT", edge.toString());
        }

        // [get all edges summarised in time window with min count] Now a bit more advanced.
        // At query time you can actually override the logic for how Gaffer
        // aggregates properties together. So by default the count property
        // is aggregated with Sum. At query time we could change that, so the
        // count property was aggregated with Max.
        // ---------------------------------------------------------
        final GetAllElements edgesSummarisedInTimeWindowWithMinCountOperation = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge("RoadUse", new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select("startDate")
                                        .execute(new IsMoreThan(MAY_01_2000, true))
                                        .select("endDate")
                                        .execute(new IsLessThan(MAY_03_2000, false))
                                        .build()
                                )
                                .groupBy() // set the group by properties to 'none'
                                .aggregator(new ElementAggregator.Builder()
                                        .select("count")
                                        .execute(new Min())
                                        .build())
                                .build())
                        .build())
                .build();

        log("GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT_WITH_MIN_COUNT_JSON", StringUtil.toString(new JSONSerialiser().serialise(edgesSummarisedInTimeWindowWithMinCountOperation, true)));

        final CloseableIterable<? extends Element> edgesSummarisedInTimeWindowWithMinCount = graph.execute(edgesSummarisedInTimeWindowWithMinCountOperation, user);
        // ---------------------------------------------------------
        log("\nEdges in 3 day time window with min count:");
        for (final Element edge : edgesSummarisedInTimeWindowWithMinCount) {
            log("GET_ALL_EDGES_SUMMARISED_IN_TIME_WINDOW_RESULT_WITH_MIN_COUNT", edge.toString());
        }

        return edgesSummarisedInTimeWindowWithMinCount;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Aggregation walkthrough = new Aggregation();
        walkthrough.run();
    }

    private static Date getDate(final String dateStr) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(dateStr);
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Unable to parse date", e);
        }
    }
}
