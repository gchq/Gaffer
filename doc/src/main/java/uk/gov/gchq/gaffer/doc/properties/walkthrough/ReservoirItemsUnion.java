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

import com.yahoo.sketches.sampling.ReservoirItemsSketch;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.properties.generator.ReservoirItemsUnionElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collections;
import java.util.Set;

public class ReservoirItemsUnion extends PropertiesWalkthrough {
    public ReservoirItemsUnion() {
        super(com.yahoo.sketches.sampling.ReservoirItemsUnion.class, "properties/reservoirItemsUnion", ReservoirItemsUnionElementGenerator.class);
    }

    public static void main(final String[] args) throws OperationException {
        new ReservoirItemsUnion().run();
    }

    @Override
    public CloseableIterable<? extends Element> run() throws OperationException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "properties/reservoirItemsUnion/schema"))
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
                        .generator(new ReservoirItemsUnionElementGenerator())
                        .input(dummyData)
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------
        log("Added the edge A-B 1000 times each time with a ReservoirItemsUnion<String> containing a random string."
                + " Also added 500 edges X-Y0, X-Y1, ..., X-Y499 each and for each an Entity on X with a"
                + " ReservoirItemsUnion<String> containing the destination node.");


        // [get red edge] Get the red edge
        // ---------------------------------------------------------
        final GetAllElements getAllEdges = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge("red")
                        .build())
                .build();
        final CloseableIterable<? extends Element> allEdges = graph.execute(getAllEdges, user);
        // ---------------------------------------------------------
        log("\nThe red edge A-B:");
        for (final Element edge : allEdges) {
            log("GET_A-B_EDGE_RESULT", edge.toString());
        }


        // [get sample for edge a b] Get the edge A-B and print out the sample of strings
        // ---------------------------------------------------------
        final GetElements query = new GetElements.Builder()
                .input(new EdgeSeed("A", "B", DirectedType.UNDIRECTED))
                .build();
        final CloseableIterable<? extends Element> edges = graph.execute(query, user);
        final Element edge = edges.iterator().next();
        final ReservoirItemsSketch<String> stringsSketch = ((com.yahoo.sketches.sampling.ReservoirItemsUnion) edge.getProperty("stringsSample"))
                .getResult();
        final String[] samples = stringsSketch.getSamples();
        final StringBuilder sb = new StringBuilder("10 samples: ");
        for (int i = 0; i < 10 && i < samples.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(samples[i]);
        }
        // ---------------------------------------------------------
        log("\nEdge A-B with a sample of the strings");
        log("GET_SAMPLE_FOR_RED_EDGE", sb.toString());


        // [get sample for entity x] Get the entity Y and print a sample of the neighbours
        // ---------------------------------------------------------
        final GetElements query2 = new GetElements.Builder()
                .input(new EntitySeed("X"))
                .build();
        final CloseableIterable<? extends Element> entities = graph.execute(query2, user);
        final Element entity = entities.iterator().next();
        final ReservoirItemsSketch<String> neighboursSketch = ((com.yahoo.sketches.sampling.ReservoirItemsUnion) entity.getProperty("neighboursSample"))
                .getResult();
        final String[] neighboursSample = neighboursSketch.getSamples();
        sb.setLength(0);
        sb.append("10 samples: ");
        for (int i = 0; i < 10 && i < neighboursSample.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(neighboursSample[i]);
        }
        // ---------------------------------------------------------
        log("\nEntity for vertex X with a sample of its neighbouring vertices");
        log("GET_SAMPLES_FOR_X_RESULT", sb.toString());
        return null;
    }
}
