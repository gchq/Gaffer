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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import com.yahoo.sketches.frequencies.LongsSketch;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator10;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collections;
import java.util.Set;

public class LoadAndQuery10 extends LoadAndQuery {
    public LoadAndQuery10() {
        super("Using LongsSketch to estimate the frequency of longs seen on an Element");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery10().run();
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
        final Set<String> dummyData = Collections.singleton("");
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new DataGenerator10())
                        .objects(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added an edge A-B 1000 times, each time with a LongsSketch containing a random long between 0 and 9.");


        // [get] Get all edges
        // ---------------------------------------------------------
        Iterable<Edge> allEdges = graph.execute(new GetAllEdges(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Edge edge : allEdges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get frequencies of 1L and 9L] Get the edge A-B and print estimates of frequencies of 1L and 9L
        // ---------------------------------------------------------
        final GetEdges<EdgeSeed> query = new GetEdges.Builder<EdgeSeed>()
                .addSeed(new EdgeSeed("A", "B", false))
                .build();
        final Iterable<Edge> edges = graph.execute(query, user);
        final Edge edge = edges.iterator().next();
        final LongsSketch longsSketch = (LongsSketch) edge.getProperty("longsSketch");
        final String estimates = "Edge A-B: 1L seen approximately " + longsSketch.getEstimate(1L)
            + " times, 9L seen approximately " + longsSketch.getEstimate(9L) + " times.";
        // ---------------------------------------------------------
        log("\nEdge A-B with estimates of the frequencies of 1 and 9");
        log("GET_FREQUENCIES_OF_1_AND_9_FOR_EDGE_A_B", estimates);
        return null;
    }
}
