/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.doc.properties.generator.TimestampSetElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.user.User;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;

public class TimestampSet extends PropertiesWalkthrough {
    public TimestampSet() {
        super(RBMBackedTimestampSet.class, "properties/timestampSet",
                TimestampSetElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new TimestampSet().run();
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "properties/timestampSet/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [add] addElements - add the edges to the graph
        // ---------------------------------------------------------
        final Set<String> dummyData = Collections.singleton("");
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new TimestampSetElementGenerator())
                        .input(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added an edge A-B 25 times, each time with a RBMBackedTimestampSet containing a random time in 2017.");


        // [get] Get all edges
        // ---------------------------------------------------------
        CloseableIterable<? extends Element> allEdges = graph.execute(new GetAllElements(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Element edge : allEdges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get the first and last timestamps and the number of timestamps for edge a b] Get the edge A-B and print out the first and last times it was active and the total number of timestamps
        // ---------------------------------------------------------
        final GetElements query = new GetElements.Builder()
                .input(new EdgeSeed("A", "B", DirectedType.UNDIRECTED))
                .build();
        final CloseableIterable<? extends Element> edges = graph.execute(query, user);
        final Element edge = edges.iterator().next();
        final uk.gov.gchq.gaffer.time.TimestampSet timestampSet = (uk.gov.gchq.gaffer.time.TimestampSet) edge.getProperty("timestampSet");
        final Instant earliest = timestampSet.getEarliest();
        final Instant latest = timestampSet.getLatest();
        final long totalNumber = timestampSet.getNumberOfTimestamps();
        final String earliestLatestNumber = "Edge A-B was first seen at " + earliest + ", last seen at " + latest
                + ", and there were " + totalNumber + " timestamps it was active.";
        // ---------------------------------------------------------
        log("\nEdge A-B with the first seen time, last seen time and the number of times it was active:");
        log("GET_FIRST_SEEN_LAST_SEEN_AND_NUMBER_OF_TIMES_FOR_EDGE_A_B", earliestLatestNumber);


        return null;
    }
}
