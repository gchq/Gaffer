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
import uk.gov.gchq.gaffer.doc.properties.generator.LongsSketchElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collections;
import java.util.Set;

public class LongsSketch extends PropertiesWalkthrough {
    public LongsSketch() {
        super(com.yahoo.sketches.frequencies.LongsSketch.class, "properties/longsSketch", LongsSketchElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new LongsSketch().run();
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "properties/longsSketch/schema"))
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
                        .generator(new LongsSketchElementGenerator())
                        .input(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added an edge A-B 1000 times, each time with a LongsSketch containing a random long between 0 and 9.");


        // [get] Get all edges
        // ---------------------------------------------------------
        final CloseableIterable<? extends Element> allEdges = graph.execute(new GetAllElements(), user);
        // ---------------------------------------------------------
        log("\nAll edges:");
        for (final Element edge : allEdges) {
            log("GET_ALL_EDGES_RESULT", edge.toString());
        }


        // [get frequencies of 1 and 9 for edge a b] Get the edge A-B and print estimates of frequencies of 1L and 9L
        // ---------------------------------------------------------
        final GetElements query = new GetElements.Builder()
                .input(new EdgeSeed("A", "B", DirectedType.UNDIRECTED))
                .build();
        final CloseableIterable<? extends Element> edges = graph.execute(query, user);
        final Element edge = edges.iterator().next();
        final com.yahoo.sketches.frequencies.LongsSketch longsSketch = (com.yahoo.sketches.frequencies.LongsSketch) edge.getProperty("longsSketch");
        final String estimates = "Edge A-B: 1L seen approximately " + longsSketch.getEstimate(1L)
                + " times, 9L seen approximately " + longsSketch.getEstimate(9L) + " times.";
        // ---------------------------------------------------------
        log("\nEdge A-B with estimates of the frequencies of 1 and 9");
        log("GET_FREQUENCIES_OF_1_AND_9_FOR_EDGE_A_B", estimates);
        return null;
    }
}
