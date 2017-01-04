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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator5;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

public class LoadAndQuery5 extends LoadAndQuery {
    public LoadAndQuery5() {
        super("Visibilities");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery5().run();
    }

    public CloseableIterable<Edge> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User basicUser = new User("basicUser");
        // ---------------------------------------------------------


        // [generate] create some edges from the data file using our data generator class
        // ---------------------------------------------------------
        final List<Element> elements = new ArrayList<>();
        final DataGenerator5 dataGenerator = new DataGenerator5();
        for (final String s : DataUtils.loadData(getData())) {
            elements.add(dataGenerator.getElement(s));
        }
        // ---------------------------------------------------------
        log("Elements generated from the data file.");
        for (final Element element : elements) {
            log("GENERATED_EDGES", element.toString());
        }
        log("");


        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------


        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final AddElements addElements = new AddElements.Builder()
                .elements(elements)
                .build();
        graph.execute(addElements, basicUser);
        // ---------------------------------------------------------
        log("The elements have been added.\n");


        log("\nNow run a simple query to get edges\n");
        // [get simple] get all the edges that contain the vertex "1"
        // ---------------------------------------------------------
        final GetEdges<EntitySeed> getEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();
        final CloseableIterable<Edge> resultsWithBasicUser = graph.execute(getEdges, basicUser);
        // ---------------------------------------------------------
        for (final Element e : resultsWithBasicUser) {
            log("GET_RELATED_EDGES_RESULT", e.toString());
        }
        log("We get nothing back");


        log("\nGet edges with the public visibility. We shouldn't see any of the private ones.\n");
        // [get public] get all the edges that contain the vertex "1"
        // ---------------------------------------------------------
        final User publicUser = new User.Builder()
                .userId("publicUser")
                .dataAuth("public")
                .build();

        final GetEdges<EntitySeed> getPublicRelatedEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        final CloseableIterable<Edge> publicResults = graph.execute(getPublicRelatedEdges, publicUser);
        // ---------------------------------------------------------
        for (final Element e : publicResults) {
            log("GET_PUBLIC_RELATED_EDGES_RESULT", e.toString());
        }


        log("\nGet edges with the private visibility. We should get the public edges too.\n");
        // [get private] get all the edges that contain the vertex "1"
        // ---------------------------------------------------------
        final User privateUser = new User.Builder()
                .userId("privateUser")
                .dataAuth("private")
                .build();

        final GetEdges<EntitySeed> getPrivateRelatedEdges = new GetEdges.Builder<EntitySeed>()
                .addSeed(new EntitySeed("1"))
                .build();

        final CloseableIterable<Edge> privateResults = graph.execute(getPrivateRelatedEdges, privateUser);
        // ---------------------------------------------------------
        for (final Element e : privateResults) {
            log("GET_PRIVATE_RELATED_EDGES_RESULT", e.toString());
        }

        return publicResults;
    }
}
