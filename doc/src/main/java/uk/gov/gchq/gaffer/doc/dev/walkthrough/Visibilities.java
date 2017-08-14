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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.dev.generator.RoadAndRoadUseWithSecurityElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;

public class Visibilities extends DevWalkthrough {
    public Visibilities() {
        super("Visibilities", "RoadAndRoadUseWithSecurity", RoadAndRoadUseWithSecurityElementGenerator.class);
    }

    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithSecurity/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User basicUser = new User("user01");
        // ---------------------------------------------------------


        // [add] Create a data generator and add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseWithSecurityElementGenerator())
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithSecurity/data.txt")))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, basicUser);
        // ---------------------------------------------------------


        log("\nNow run a simple query to get edges\n");
        // [get simple] get all the edges that contain the vertex "10" or "23"
        // ---------------------------------------------------------
        final GetElements getEdges = new GetElements.Builder()
                .input(new EntitySeed("10"), new EntitySeed("23"))
                .view(new View.Builder()
                        .edge("RoadUse")
                        .build())
                .build();
        final CloseableIterable<? extends Element> resultsWithBasicUser = graph.execute(getEdges, basicUser);
        // ---------------------------------------------------------
        for (final Element e : resultsWithBasicUser) {
            log("GET_ELEMENTS_RESULT", e.toString());
        }
        log("We get nothing back");


        log("\nGet edges with the public visibility. We shouldn't see any of the private ones.\n");
        // [get public] get all the edges that contain the vertex "10" or "23"
        // ---------------------------------------------------------
        final User publicUser = new User.Builder()
                .userId("publicUser")
                .dataAuth("public")
                .build();

        final GetElements getPublicRelatedEdges = new GetElements.Builder()
                .input(new EntitySeed("10"), new EntitySeed("23"))
                .view(new View.Builder()
                        .edge("RoadUse")
                        .build())
                .build();

        final CloseableIterable<? extends Element> publicResults = graph.execute(getPublicRelatedEdges, publicUser);
        // ---------------------------------------------------------
        for (final Element e : publicResults) {
            log("GET_PUBLIC_EDGES_RESULT", e.toString());
        }


        log("\nGet edges with the private visibility. We should get the public edges too.\n");
        // [get private] get all the edges that contain the vertex "10" or "23"
        // ---------------------------------------------------------
        final User privateUser = new User.Builder()
                .userId("privateUser")
                .dataAuth("private")
                .build();

        final GetElements getPrivateRelatedEdges = new GetElements.Builder()
                .input(new EntitySeed("10"), new EntitySeed("23"))
                .view(new View.Builder()
                        .edge("RoadUse")
                        .build())
                .build();

        final CloseableIterable<? extends Element> privateResults = graph.execute(getPrivateRelatedEdges, privateUser);
        // ---------------------------------------------------------
        for (final Element e : privateResults) {
            log("GET_PRIVATE_EDGES_RESULT", e.toString());
        }

        return publicResults;
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Visibilities walkthrough = new Visibilities();
        walkthrough.run();
    }
}
